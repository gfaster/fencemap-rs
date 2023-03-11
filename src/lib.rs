#![allow(dead_code)]
use std::cell::UnsafeCell;
use std::ops::IndexMut;
use std::ops::Range;
use std::ops::Rem;
use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::RwLock;
use std::thread;

const QUEUE_SIZE: usize = 32;

struct FixedQueue<T, const S: usize> {
    q: UnsafeCell<[T; S]>,
    cons: AtomicUsize,
    prod: Mutex<usize>,
    spce: Condvar,
}

impl<T, const S: usize> FixedQueue<T, { S }> {
    unsafe fn append(&self, item: T) -> usize {

        let mut lock = self.spce.wait_while(self.prod.lock().unwrap(), |prod| {
            *prod - self.cons.load(atomic::Ordering::Relaxed) == S
        }).unwrap();

        let idx = lock.rem(S);
        unsafe {
            self.q.get().as_mut().unwrap()[idx] = item;
        }

        *lock += 1;
        *lock
    }

    

    fn try_pop(&mut self) -> Option<T> {

    }
}

struct Fence<'a, T> {
    resp: &'a [T],
    idx: usize,
    bnds: Range<usize>,
}

trait Fenced<T> {}
