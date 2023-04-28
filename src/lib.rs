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
    Update(K, V),
    Remove(K),
    Fetch(K, Arc<Mutex<Option<Arc<V>>>>),
    StartUpdateLeft(K),
    FinishUpdateLeft(K),
    StartUpdateRight(K),
    FinishUpdateRight(K),
}

/// A Fence is a worker responsible for a part of the set
struct Fence<K, V> where K: Ord {
    /// The set this fence is responsible for
    main: BTreeMap<K, Arc<V>>,

    left: Option<mpsc::Sender<Operation<K, V>>>,
    right: Option<mpsc::Sender<Operation<K, V>>>,
    queue: mpsc::Receiver<Operation<K, V>>
}

impl<K: Ord, V> Fence<K, V> {

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
    val: Arc<Mutex<Option<Arc<V>>>>
}

impl<V> Future for ReadFuture<V> {
    type Output = Arc<V>;

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
