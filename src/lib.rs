#![allow(dead_code)]
use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::task::Poll;
use std::thread;
use std::time::Instant;

const MAX_OP_PER_SECOND: usize = 10_000_000;

enum Operation<V> {
    Insert(u64, V),
    Remove(u64),
    Fetch(u64, Arc<(Condvar, Mutex<Option<Option<Arc<V>>>>)>),
    AddLeft(Sender<Operation<V>>),
    AddRight(Sender<Operation<V>>),
    RemoveLeft,
    RemoveRight,
    UpdateLower(u64),
    UpdateUpper(u64),
    NOP,
    Drop
}

/// A Fence is a worker responsible for a part of the set
struct Fence<V> {
    /// The set this fence is responsible for
    resp: BTreeMap<u64, Arc<V>>,

    id: usize,

    left:  Option<Sender<Operation<V>>>,
    right: Option<Sender<Operation<V>>>,
    queue: Receiver<Operation<V>>,
    bounds: Range<u64>,
    ops_since_rebalance: u64,
    last_rebalance: Instant
}

impl<V> Fence<V> {
    fn new(rx: Receiver<Operation<V>>, bounds: Range<u64>, id: usize) -> Self {
        Self { resp: Default::default(), left: None, right: None, queue: rx, bounds, ops_since_rebalance: 0, last_rebalance: Instant::now(), id}
    }

    fn execute(&mut self) {
        loop {
            let op = self.queue.recv().unwrap_or(Operation::NOP);
            self.ops_since_rebalance += 1;
            match op {
                Operation::Insert(key, val) => self.insert(key, val),
                Operation::Remove(key) => self.remove(key),
                Operation::Fetch(key, av) => self.fetch(key, av),
                Operation::AddLeft(l) => self.left = Some(l),
                Operation::AddRight(r) => self.right = Some(r),
                Operation::RemoveLeft => self.left = None,
                Operation::RemoveRight => self.right = None,
                Operation::UpdateLower(r) => self.bounds.start = r,
                Operation::UpdateUpper(r) => self.bounds.end = r,
                Operation::NOP => (),
                Operation::Drop => return,
            }
        }
    }

    /// Rebalancing the fence partitions seems like it will be quite hard.
    ///
    /// For a race-free rebalance, the operation must occur in the following ordering:
    /// 1. A fence decides to rebalance. It sends a request to the adjacent node containing the
    ///    removed range and the values associated with it. All read and write requests in the
    ///    removed range are sent to the adjacent node.
    /// 2. The new node recieves the request and inserts it into its tree. It updates the official
    ///    ranges.
    ///
    /// Fences must be careful not to send rebalancing requests to each other simultaneously.
    fn rebalance(&mut self) {
        todo!()
    }

    fn insert(&mut self, key: u64, val: V) {
        // eprintln!("Insert {key} at in fence {}", self.id);
        if self.bounds.contains(&key) {
            self.resp.insert(key, val.into());
        } else if key < self.bounds.start {
            self.left.as_mut().expect("key less than bounds implies left fence").send(Operation::Insert(key, val)).unwrap();
        } else {
            self.right.as_mut().expect("key gte bounds implies right fence").send(Operation::Insert(key, val)).unwrap();
        }
    }

    fn remove(&mut self, key: u64) {
        if self.bounds.contains(&key) {
            self.resp.remove(&key);
        } else if key < self.bounds.start {
            self.left.as_mut().expect("key less than bounds implies left fence").send(Operation::Remove(key)).unwrap();
        } else {
            self.right.as_mut().expect("key gte bounds implies right fence").send(Operation::Remove(key)).unwrap();
        }
    }

    fn fetch(&mut self, key: u64, dest: Arc<(Condvar, Mutex<Option<Option<Arc<V>>>>)>) {
        // eprintln!("Fetch {key} at in fence {}", self.id);
        if self.bounds.contains(&key) {
            let mut d = dest.1.lock().unwrap();
            if let Some(res) = self.resp.get(&key) {
                d.replace(Some(res.clone()));
            } else {
                d.replace(None);
            }
            dest.0.notify_one();
        } else if key < self.bounds.start {
            self.left.as_mut().expect("key less than bounds implies left fence").send(Operation::Fetch(key, dest)).unwrap();
        } else {
            self.right.as_mut().expect("key gte bounds implies right fence").send(Operation::Fetch(key, dest)).unwrap();
        }

    }
}


pub struct FenceMap<const F: usize, V> {
    fences: [mpsc::Sender<Operation<V>>; F],

    /// The dividing line between fences, as a half-open interval, unbounded at the end. In
    /// practice, it's actually `u64::MAX`, exclusive.
    ///
    /// A job goes to the first fence where `posts[idx] <= key`
    posts: [AtomicU64; F],
}

impl<const F: usize, V> FenceMap<F, V> 
where 
    V: Send + Sync + 'static 
{
    pub fn new() -> Self {
        let interval = u64::MAX / F as u64;
        let posts = core::array::from_fn(|i| (i as u64 * interval).into());

        let fences = core::array::from_fn(|i| {
            let (fencetx, fencerx) = mpsc::channel();

            let bounds = (i as u64 * interval)..{
                if i != F - 1 {
                    (i as u64 + 1) * interval
                } else  {
                    u64::MAX
                }
            };
            eprintln!("Fence {i} has bounds {bounds:?}");
            let mut fence: Fence<V> = Fence::new(fencerx, bounds, i);
            thread::spawn(move || {
                fence.execute();
            });
            fencetx
        });

        fences.windows(2).map(|s| match s {
            [l, r] => {
                l.send(Operation::AddRight(r.clone())).unwrap();
                r.send(Operation::AddLeft(l.clone())).unwrap();
            },
            _ => panic!("windows 2 should only have 2")
        }).last();

        let ret = Self { 
            fences,
            posts
        };

        ret
    }


    pub fn reader(&self) -> FenceReader<F, V> {
        FenceReader {
            fences: self.fences.clone(),
            posts: &self.posts,
        }
    }

    pub fn writer(&self) -> FenceWriter<F, V> {
        FenceWriter {
            fences: self.fences.clone(),
            posts: &self.posts,
        }
    }
}

impl<const F: usize, T> Drop for FenceMap<F, T> {
    fn drop(&mut self) {
        for fence in &self.fences {
            fence.send(Operation::Drop).unwrap();
        }
    }
}

#[inline]
fn find_post<const F: usize>(posts: &[AtomicU64; F], key: u64) -> usize {
    posts.iter().enumerate().find(|(_, x)| x.load(std::sync::atomic::Ordering::Relaxed) > key).map_or(F - 1, |(i, _)| i - 1)
}

pub struct FenceWriter<'a, const F: usize, V> {
    fences: [Sender<Operation<V>>; F],
    posts: &'a [AtomicU64; F]
}

impl<const F: usize, V> FenceWriter<'_, F, V> {
    pub fn insert(&self, key: u64, val: V) {
        let post = find_post(self.posts, key);
        self.fences[post].send(Operation::Insert(key, val)).unwrap();
    }

    pub fn delete(&self, key: u64) {
        let post = find_post(self.posts, key);
        self.fences[post].send(Operation::Remove(key)).unwrap();
    }
}


pub struct FenceReader<'a, const F: usize, V> {
    fences: [Sender<Operation<V>>; F],
    posts: &'a [AtomicU64; F]
}

pub struct ReadFuture<V> {
    val: Arc<(Condvar, Mutex<Option<Option<Arc<V>>>>)>,
}

impl<V> Future for ReadFuture<V> {
    type Output = Option<Arc<V>>;

    fn poll(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let Some(mut guard) = self.val.1.try_lock().ok() else { return Poll::Pending };
        match guard.take() {
            Some(val) => Poll::Ready(val),
            None => Poll::Pending,
        }
    }
}

impl<const F: usize, V> FenceReader<'_, F, V>{
    pub async fn read_async(&self, key: u64) -> ReadFuture<V> {
        let rf = ReadFuture { 
            val: Arc::new((Condvar::new(), Mutex::new(None))),
        };
        let post = find_post(self.posts, key);
        self.fences[post].send(Operation::Fetch(key, rf.val.clone())).unwrap();
        rf
    }

    pub fn read(&self, key: u64) -> Option<Arc<V>> {
        let res = Arc::new((Condvar::new(), Mutex::new(None)));
        let post = find_post(self.posts, key);
        self.fences[post].send(Operation::Fetch(key, res.clone())).unwrap();
        
        let (cvar, lock) = &*res;
        let guard = cvar.wait_while(lock.lock().unwrap(), |wait| wait.is_none()).unwrap();
        guard.as_ref().expect("waited until read is done").clone()
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;

    #[test]
    fn trivial_insert_read() {
        let map: FenceMap<1, i32> = FenceMap::new();

        let writer = map.writer();
        let reader = map.reader();

        writer.insert(17, 22);
        let val = reader.read(17).unwrap();
        assert_eq!(22, *val);
    }

    #[test]
    fn trivial_multiinsert_read() {
        let map: FenceMap<1, i32> = FenceMap::new();

        let writer = map.writer();
        let reader = map.reader();

        writer.insert(17, 22);
        writer.insert(18, 23);
        assert_eq!(22, *reader.read(17).unwrap());
    }

    #[test]
    fn dual_multiinsert_read() {
        let map: FenceMap<2, i32> = FenceMap::new();

        let writer = map.writer();
        let reader = map.reader();

        writer.insert(17, 22);
        writer.insert(18, 23);
        assert_eq!(22, *reader.read(17).unwrap());
        assert_eq!(23, *reader.read(18).unwrap());
    }

    #[test]
    fn dual_multichannel() {
        let map: FenceMap<2, i32> = FenceMap::new();

        let writer = map.writer();
        let reader = map.reader();

        let key_high = u64::MAX - 18;

        writer.insert(17, 22);
        writer.insert(key_high, 23);
        assert_eq!(22, *reader.read(17).unwrap());
        assert_eq!(23, *reader.read(key_high).unwrap());
    }

    #[test]
    fn many_channel() {
        let map = Box::leak(Box::new(FenceMap::<25, i32>::new()));

        let keys: Arc<Vec<_>> = (0..1000).map(|i| i * (i32::MAX as u64 / 6000)).collect::<Vec<_>>().into();
        let chunk_cnt = keys.chunks(300).count();

        let threads: Vec<_> = (0..chunk_cnt).map(|i| {
            let keys = keys.clone();
            let writer = map.writer();
            thread::spawn(move || {
                let chunk = keys.chunks(3000).collect::<Vec<_>>()[i];
                chunk.iter().for_each( |key| writer.insert(*key, *key as i32 + 18))
            })
        }).collect();

        threads.into_iter().map(|t| t.join()).last();

        thread::sleep(Duration::new(0, 100_000_000));

        let reader = map.reader();

        for key in keys.iter() {
            assert_eq!(*key as i32 + 18, *reader.read(*key).unwrap());
        }
    }


    #[test]
    fn very_many_channel() {
        let map = Box::leak(Box::new(FenceMap::<250, usize>::new()));

        let range = 0..3000;
        let chunk_cnt = 3000;

        let thread_writers: Vec<_> = (0..chunk_cnt).map(|i| {
            let writer = map.writer();
            let range_thread = range.clone();
            thread::spawn(move || {
                let chunk = (i * range_thread.len())..((i + 1) * range_thread.len());
                chunk.for_each( |key| writer.insert(key as u64, key + 18))
            })
        }).collect();
        thread_writers.into_iter().map(|t| t.join().unwrap()).last();

        thread::sleep(Duration::new(0, 100_000_000));

        let thread_readers: Vec<_> = (0..chunk_cnt).map(|i| {
            let reader = map.reader();
            let range_thread = range.clone();
            thread::spawn(move || {
                let chunk = (i * range_thread.len())..((i + 1) * range_thread.len());
                chunk.for_each( |key| assert_eq!(key + 18, *reader.read(key as u64).unwrap()))
            })
        }).collect();
        thread_readers.into_iter().map(|t| t.join().unwrap()).last();
    }
}
