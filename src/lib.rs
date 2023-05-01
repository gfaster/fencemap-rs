#![allow(dead_code)]
use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::task::Poll;
use std::thread;
use std::time::Instant;


enum Operation<V> {
    Insert(u64, V),
    Remove(u64),
    Fetch(u64, Arc<Mutex<Option<Option<Arc<V>>>>>),
    AddLeft(Sender<Operation< V>>),
    AddRight(Sender<Operation< V>>),
    RemoveLeft,
    RemoveRight,
    UpdateLower(u64),
    UpdateUpper(u64),
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
            let op = self.queue.recv().expect("fence worker can recieve");
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

    fn fetch(&mut self, key: u64, dest: Arc<Mutex<Option<Option<Arc<V>>>>>) {
        if self.bounds.contains(&key) {
            let mut d = dest.lock().unwrap();
            if let Some(res) = self.resp.get(&key) {
                d.replace(Some(res.clone()));
            } else {
                d.replace(None);
            }
        } else if key < self.bounds.start {
            self.left.as_mut().expect("key less than bounds implies left fence").send(Operation::Fetch(key, dest)).unwrap();
        } else {
            self.right.as_mut().expect("key gte bounds implies right fence").send(Operation::Fetch(key, dest)).unwrap();
        }

    }
}


struct FenceMap<const F: usize, V> {
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

            let bounds = (i as u64 * interval)..(i as u64 * (interval + 1));
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
            fences: &self.fences,
            posts: &self.posts,
        }
    }

    pub fn writer(&self) -> FenceWriter<F, V> {
        FenceWriter {
            fences: &self.fences,
            posts: &self.posts,
        }
    }
}

/// This should eventually be updated to reference the list of fences
struct FenceWriter<'a, const F: usize, V> {
    fences: &'a [Sender<Operation<V>>; F],
    posts: &'a [AtomicU64]
}


struct FenceReader<'a, const F: usize, V> {
    fences: &'a [Sender<Operation<V>>; F],
    posts: &'a [AtomicU64]
}

struct ReadFuture<V> {
    val: Arc<Mutex<Option<Option<Arc<V>>>>>
}

impl<V> Future for ReadFuture<V> {
    type Output = Option<Arc<V>>;

    fn poll(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let Some(mut guard) = self.val.try_lock().ok() else { return Poll::Pending };
        match guard.take() {
            Some(val) => Poll::Ready(val),
            None => Poll::Pending,
        }
    }
}

impl<const F: usize, V> FenceReader<'_, F, V>{
    async fn read(&self, key: u64) -> ReadFuture<V> {
        let rf = ReadFuture { 
            val: Arc::new(Mutex::new(None))
        };

        let idx = self.posts.iter().enumerate().find(|(_, x)| x.load(std::sync::atomic::Ordering::Relaxed) >= key).unwrap().0;
        self.fences[idx].send(Operation::Fetch(key, rf.val.clone())).unwrap();
        rf
    }
}
