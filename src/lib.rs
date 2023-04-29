#![allow(dead_code)]
use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::task::Poll;
use std::thread;


enum Operation<K, V> {
    Insert(K, V),
    Remove(K),
    Fetch(K, Arc<Mutex<Option<Option<Arc<V>>>>>),
    AddLeft(Sender<Operation<K, V>>),
    AddRight(Sender<Operation<K, V>>),
    RemoveLeft,
    RemoveRight,
    UpdateRange(Range<K>),
    Drop
}

/// A Fence is a worker responsible for a part of the set
struct Fence<K, V> where K: Ord {
    /// The set this fence is responsible for
    resp: BTreeMap<K, Arc<V>>,

    left:  Option<Sender<Operation<K, V>>>,
    right: Option<Sender<Operation<K, V>>>,
    reject: Sender<Operation<K, V>>,
    queue: Receiver<Operation<K, V>>,
    bounds: Range<K>
}

impl<K: Ord, V> Fence<K, V> {
    fn new(rx: Receiver<Operation<K,V>>, reject: Sender<Operation<K, V>>, bounds: Range<K>) -> Self {
        Self { resp: Default::default(), left: None, right: None, queue: rx, bounds, reject}
    }

    fn execute(&mut self) {
        self.execute_op(self.queue.recv().expect("fence worker can recieve"))
    }

    fn execute_op(&mut self, op: Operation<K, V>) {
        match op {
            Operation::Insert(key, val) => self.insert(key, val),
            Operation::Remove(key) => self.remove(key),
            Operation::Fetch(key, av) => self.fetch(key, av),
            Operation::AddLeft(l) => self.left = Some(l),
            Operation::AddRight(r) => self.right = Some(r),
            Operation::RemoveLeft => todo!(),
            Operation::RemoveRight => todo!(),
            Operation::UpdateRange(_) => todo!(),
            Operation::Drop => todo!(),
        }
    }

    fn insert(&mut self, key: K, val: V) {
        self.resp.insert(key, val.into());
    }

    fn remove(&mut self, key: K) {
        self.resp.remove(&key);
    }

    fn fetch(&mut self, key: K, dest: Arc<Mutex<Option<Option<Arc<V>>>>>) {
        let mut d = dest.lock().unwrap();
        if let Some(res) = self.resp.get(&key) {
            d.replace(Some(res.clone()));
        } else {
            d.replace(None);
        }
    }
}


struct FenceMap<K, V> where K: Ord {
    fences: Vec<mpsc::Sender<Operation<K, V>>>,
    incoming: mpsc::Receiver<Operation<K, V>>,
    bounds: Range<K>,
    recv: mpsc::Sender<Operation<K, V>>
}

impl<K, V> FenceMap<K, V> where K: Ord + Send + 'static + Default, V: Send + Sync + 'static {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        let mut ret = Self { 
            fences: Default::default(),
            incoming: rx,
            recv: tx,
            bounds: Default::default()
        };

        ret.add_fence(Default::default());

        ret
    }

    fn add_fence(&mut self, bounds: Range<K>) {
        let (tx, rx) = mpsc::channel();

        let mut fence: Fence<K, V> = Fence {
            resp: Default::default(),
            left: None,
            right: None,
            queue: rx,
            bounds,
            reject: self.recv.clone()
        };

        if let Some(ref mut last) = self.fences.last_mut() {
            fence.left = Some(last.clone());
        }

        thread::spawn(move || {
            loop {
                fence.execute();
            }
        });

        self.fences.push(tx);
    }

    pub fn reader(&self) -> FenceReader<K, V> {
        FenceReader { sender: self.recv.clone() }
    }

    pub fn writer(&self) -> FenceWriter<K, V> {
        FenceWriter { sender: self.recv.clone() }
    }
}

/// This should eventually be updated to reference the list of fences
struct FenceWriter<K, V> where K: Ord {
    sender: Sender<Operation<K, V>>
}


struct FenceReader<K, V> where K: Ord {
    sender: Sender<Operation<K, V>>
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

impl<K, V> FenceReader<K, V> where K: Ord {
    async fn read(&self, key: K) -> ReadFuture<V> {
        let rf = ReadFuture { 
            val: Arc::new(Mutex::new(None))
        };
        self.sender.send(Operation::Fetch(key, rf.val.clone())).unwrap();
        rf
    }
}
