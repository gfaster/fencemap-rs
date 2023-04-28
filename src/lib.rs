#![allow(dead_code)]
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicPtr;
use std::sync::mpsc;
use std::task::Poll;
use std::thread;


enum Operation<K, V> {
    Insert(K, V),
    Remove(K),
    Fetch(K, Arc<Mutex<Option<Option<Arc<V>>>>>),
    StartUpdateLeft(K),
    FinishUpdateLeft(K),
    StartUpdateRight(K),
    FinishUpdateRight(K),
}

/// A Fence is a worker responsible for a part of the set
struct Fence<K, V> where K: Ord {
    /// The set this fence is responsible for
    resp: BTreeMap<K, Arc<V>>,

    left: Option<mpsc::Sender<Operation<K, V>>>,
    right: Option<mpsc::Sender<Operation<K, V>>>,
    queue: mpsc::Receiver<Operation<K, V>>
}

impl<K: Ord, V> Fence<K, V> {
    fn execute_op(&mut self, op: Operation<K, V>) {
        match op {
            Operation::Insert(key, val) => self.insert(key, val),
            Operation::Remove(_) => todo!(),
            Operation::Fetch(key, av) => self.fetch(key, av),
            Operation::StartUpdateLeft(_) => todo!(),
            Operation::FinishUpdateLeft(_) => todo!(),
            Operation::StartUpdateRight(_) => todo!(),
            Operation::FinishUpdateRight(_) => todo!(),
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


struct FenceSet<'a, K, V> where K: Ord {
    readers: BTreeSet<Arc<FenceReader<'a, K, V>>>,
    fences: BTreeMap<Range<K>, Fence<K, V>>,
    incoming: mpsc::Receiver<Operation<K, V>>,
}

struct FenceReader<'a, K, V> where K: Ord {
    set: &'a FenceSet<'a, K, V>,
    sender: mpsc::Sender<Operation<K, V>>
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

impl<'a, K, V> FenceReader<'a, K, V> where K: Ord {
    async fn read(&self, key: K) -> ReadFuture<V> {
        let rf = ReadFuture { 
            val: Arc::new(Mutex::new(None))
        };
        self.sender.send(Operation::Fetch(key, rf.val.clone())).unwrap();
        rf
    }
}
