# FenceMap-rs
FenceMap is a multithreaded, BTree-based map that has balanced read and write
speeds.

**The current iteration does not implement balancing, and therefore should not
be used**

## How it works

FenceMap works by partitioning keys and giving each partition to a thread worker
referred to as a "fence". All readers and writers first find the fence that
contains the relevant key, and send the operation to that particular fence. If a
fence is overwhelmed with requests, it will rebalance by sending some of its
partition to a neighboring, less burdened fence.

Here's a simplified example. Imagine a 2-fence FenceMap is used over 10 keys. At
first, the fences look like this:

```
| 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | <-- keys
 |---- Fence 1 ----| |---- Fence 2 ----|
```

However, after some time, Fence 2 realizes it has significantly more traffic
than fence 1. To better handle this traffic, Fence 2 will initiate rebalancing
and give responsibility of some of its keys to Fence 1.

```
| 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | <-- keys
 |-------- Fence 1 --------| | Fence 2 |
```

Now that Fence 1 has responsibility for the "long tail" of accesses, Fence 2 can
dedicate its CPU time on a smaller range of keys. 

## Use cases
FenceMap occupies a very small niche. For maximum effectiveness, the following
constraints should be met:

1. Data access is non-uniform. This means that hashed keys will be unable to
   fully realize potential gains 

2. Keys do not steadily increment. In these cases, the lead fence will
   constantly be balancing, leading to a backwards precession of balances - a
   rather expensive operation at present.

3. Accesses are not heavily biased towards reads. Data structures that offer
   blazzingly fast reads at the expense of writes are plentiful, but this is not
   one of them.

4. A total ordering of operations is required (subject to change). All writes
   that happen before a read (I think) are guaranteed to be visible to the
   reader.

5. It is beneficial for keys to be sorted. The underlying BTree means that keys
   are implicitly sorted. At the moment, there is no way for the user to take
   advantage of this property.

Not meeting any of these is not a deal breaker, but it's not where FenceMap
shines.

These sound severe, so what is the benefit of FenceMap?

Close to zero thread overhead. The only operation that has any sort of
contention is the location of fences. With ideal data access patterns, throwing
more cores at it should always make it faster.

## Future Improvements

Currently, there is no rebalancing. FenceMap is functional, but offers no
improvements over a standard BTree.

FenceMap is currently written entirely in safe Rust. This is great for being
memory-safe, but it requires many concessions in speed. Every value in the
data structure is held in an `Arc`. Creating a implementation that does not rely
on reference counting is semantically valid, and a goal for future work.
