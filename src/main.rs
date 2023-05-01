use std::time::Duration;
use fencemap::FenceMap;
use std::thread;


fn main() {
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
