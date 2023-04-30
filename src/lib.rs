#![allow(dead_code)]
use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Add;
use std::ops::AddAssign;
use std::ops::Range;
use std::ops::Sub;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::task::Poll;
use std::thread;


enum Operation<V> {
    Insert(u64, V),
    Remove(u64),
    Fetch(u64, Arc<Mutex<Option<Option<Arc<V>>>>>),
    AddLeft(Sender<Operation< V>>),
    AddRight(Sender<Operation< V>>),
    RemoveLeft,
    RemoveRight,
    UpdateRange(Range<u64>),
    Drop
}

/// A Fence is a worker responsible for a part of the set
struct Fence<V> {
    /// The set this fence is responsible for
    resp: BTreeMap<u64, Arc<V>>,

    left:  Option<Sender<Operation<V>>>,
    right: Option<Sender<Operation<V>>>,
    reject: Sender<Operation<V>>,
    queue: Receiver<Operation<V>>,
    bounds: Range<u64>
}

impl<V> Fence<V> {
    fn new(rx: Receiver<Operation<V>>, reject: Sender<Operation<V>>, bounds: Range<u64>) -> Self {
        Self { resp: Default::default(), left: None, right: None, queue: rx, bounds, reject}
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
                Operation::UpdateRange(r) => self.bounds = r,
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
        } else {
            self.reject.send(Operation::Insert(key, val)).unwrap();
        }
    }

    fn remove(&mut self, key: u64) {
        if self.bounds.contains(&key) {
            self.resp.remove(&key);
        } else {
            self.reject.send(Operation::Remove(key)).unwrap();
        }
    }

    fn fetch(&mut self, key: u64, dest: Arc<Mutex<Option<Option<Arc<V>>>>>) {
        let mut d = dest.lock().unwrap();
        if let Some(res) = self.resp.get(&key) {
            d.replace(Some(res.clone()));
        } else {
            d.replace(None);
        }
    }
}


struct FenceMap<V> {
    fences: Vec<mpsc::Sender<Operation<V>>>,
    incoming: mpsc::Receiver<Operation<V>>,
    bounds: Range<u64>,
    recv: mpsc::Sender<Operation<V>>
}

impl<V> FenceMap<V> 
where 
    V: Send + Sync + 'static 
{
    pub fn new(bounds: Range<u64>) -> Self {
        let (tx, rx) = mpsc::channel();
        let mut ret = Self { 
            fences: Default::default(),
            incoming: rx,
            recv: tx,
            bounds: bounds.clone()
        };


        ret.add_fence(bounds);

        ret
    }

    fn process(&mut self) {
        let op = self.incoming.recv().unwrap();
        match op {
            Operation::Insert(_, _) => todo!(),
            Operation::Remove(_) => todo!(),
            Operation::Fetch(_, _) => todo!(),
            Operation::AddLeft(_) => todo!(),
            Operation::AddRight(_) => todo!(),
            Operation::RemoveLeft => todo!(),
            Operation::RemoveRight => todo!(),
            Operation::UpdateRange(_) => todo!(),
            Operation::Drop => todo!(),
        }
    }

    fn add_fence(&mut self, bounds: Range<u64>) {
        let (tx, rx) = mpsc::channel();


        let mut fence: Fence<V> = Fence {
            resp: Default::default(),
            left: None,
            right: None,
            queue: rx,
            bounds,
            reject: self.recv.clone()
        };

        if let Some(ref mut last) = self.fences.last_mut() {
            fence.left = Some(last.clone());
            last.send(Operation::AddRight(tx.clone())).unwrap();
        }

        thread::spawn(move || {
            fence.execute();
        });

        self.fences.push(tx);
    }

    pub fn reader(&self) -> FenceReader<V> {
        FenceReader { sender: self.recv.clone() }
    }

    pub fn writer(&self) -> FenceWriter<V> {
        FenceWriter { sender: self.recv.clone() }
    }
}

/// This should eventually be updated to reference the list of fences
struct FenceWriter<V> {
    sender: Sender<Operation<V>>
}


struct FenceReader<V> {
    sender: Sender<Operation<V>>
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

impl<V> FenceReader<V>{
    async fn read(&self, key: u64) -> ReadFuture<V> {
        let rf = ReadFuture { 
            val: Arc::new(Mutex::new(None))
        };
        self.sender.send(Operation::Fetch(key, rf.val.clone())).unwrap();
        rf
    }
}
