Handoff Document: Optimizing nfs-walker for Narrow-Deep Directory Structures
Context
nfs-walker is a high-performance NFS filesystem scanner written in Rust that uses libnfs directly (bypassing kernel NFS client) with READDIRPLUS for efficient metadata retrieval. It uses BFS traversal with work-stealing parallelism and writes results to RocksDB.
Repository: https://github.com/blakegolliher/nfs-walker
The Problem
Benchmark results show a severe performance regression on narrow-deep directory structures:
ScenarioFilesDirsDepthdudustnfs-walkernarrow-deep7,000701~7003.91s5.83s51.61smix500,00010,001~3222.6s16.55s3.76s
nfs-walker is 13x slower than du on narrow-deep, but 59x faster on mixed workloads. The BFS + work-stealing approach that makes it excellent for wide trees becomes a liability on deep, narrow trees.
Root Cause Analysis
The current architecture processes directories like this:
BFS Queue: [/root]
Worker1 takes /root → finds /root/level1 → pushes to queue
BFS Queue: [/root/level1]  
Worker1 takes /root/level1 → finds /root/level1/level2 → pushes to queue
BFS Queue: [/root/level1/level2]
... (701 sequential operations)
Problems:

No parallelism possible — each level has only 1 subdirectory, so only 1 worker is ever busy
Per-directory overhead — libnfs READDIRPLUS call overhead repeated 701 times sequentially
Work queue overhead — pushing/popping from crossbeam channel 701 times for no benefit
16 workers idle — 15 workers sitting idle waiting for work that never comes

Performance Target

Goal: Match or approach dust's performance (5.83s)
Acceptable: Within 2x of dust (~12s) given we're writing to RocksDB
Current: 51.61s (8.8x slower than dust)

Proposed Solutions to Explore
Option 1: Hybrid BFS/DFS with Narrow-Path Detection
Detect when a directory has only 1 subdirectory and switch to inline DFS processing instead of queueing:
rust// Pseudocode
fn process_directory(task: DirTask) {
    let entries = nfs.readdir_plus(&task.path)?;
    let subdirs: Vec<_> = entries.iter().filter(|e| e.is_dir()).collect();
    
    if subdirs.len() == 1 {
        // Narrow path detected - process inline (DFS)
        // Don't push to queue, just recurse directly
        process_directory_inline(subdirs[0]);
    } else {
        // Wide path - use normal BFS queue for parallelism
        for subdir in subdirs {
            work_tx.send(subdir)?;
        }
    }
}
Pros: Minimal code change, preserves BFS benefits for wide trees
Cons: Still has per-directory libnfs overhead
Option 2: Path Prefetching / Speculative Descent
When detecting a narrow path, speculatively descend multiple levels in a single worker before returning:
rustfn process_with_lookahead(task: DirTask, max_depth: usize) {
    let mut current = task;
    let mut depth = 0;
    
    while depth < max_depth {
        let entries = nfs.readdir_plus(&current.path)?;
        let subdirs: Vec<_> = entries.iter().filter(|e| e.is_dir()).collect();
        
        // Process files at this level
        for file in entries.iter().filter(|e| !e.is_dir()) {
            result_tx.send(file)?;
        }
        
        if subdirs.len() == 1 {
            // Continue descending inline
            current = DirTask::new(subdirs[0].path);
            depth += 1;
        } else {
            // Hit a wide point - queue remaining work and exit
            for subdir in subdirs {
                work_tx.send(subdir)?;
            }
            break;
        }
    }
}
Pros: Batches narrow sections, returns to BFS at branch points
Cons: More complex state management
Option 3: Parallel Chain Detection
On initial scan, detect "chains" (sequences of single-child directories) and assign entire chains to single workers:
ruststruct DirTask {
    path: String,
    depth: u32,
    is_chain: bool,        // New: process this path and all single-child descendants
    chain_depth_limit: u32, // New: how deep to go before re-evaluating
}
Pros: Explicit optimization for the narrow case
Cons: Requires two-pass or speculative detection
Option 4: Reduce Per-Directory Overhead
Profile where time is actually spent. Possibilities:

libnfs connection/context overhead per call
RocksDB write batching not optimal for sequential inserts
Crossbeam channel contention even with single producer
String allocation for path building

bash# Profile with flamegraph
cargo flamegraph --root -- nfs-walker nfs://server/narrow-deep -o /tmp/test.rocks
```

**Pros:** May find quick wins independent of traversal strategy
**Cons:** May not be enough alone

### Option 5: NFS Compound Operations (NFSv4)

If using NFSv4, investigate using COMPOUND operations to batch multiple READDIR calls:
```
COMPOUND {
    PUTFH(root)
    READDIR
    LOOKUP(subdir)
    READDIR
    LOOKUP(subdir)
    READDIR
    ...
}
Pros: Reduces round trips dramatically
Cons: Complex to implement, NFSv4 specific, may not be supported by libnfs easily
Files to Examine
Based on our previous design discussions, key files are likely:

src/walker.rs — Main traversal logic, work queue management
src/nfs/connection.rs — libnfs wrapper, READDIRPLUS calls
src/db/writer.rs — RocksDB batched writer
src/main.rs — CLI and worker pool setup

Suggested Implementation Order

Profile first — Run flamegraph on narrow-deep to see where time actually goes
Implement Option 1 — Hybrid BFS/DFS is lowest-risk change
Benchmark — See if it gets us close to target
If needed, try Option 2 — Speculative descent for further gains
Consider Option 4 — Look for per-call overhead reductions

Testing the Fix
bash# Benchmark command for narrow-deep
time sudo ./nfs-walker-rocks nfs://172.200.203.1/syncengine-demo/bench/narrow-deep -q

# Compare against
time dust -d 0 /mnt/syncengine-demo/bench/narrow-deep  # Target: ~6s
time du -sh /mnt/syncengine-demo/bench/narrow-deep      # Target: ~4s

# CRITICAL: Also verify no regression on mix (should stay ~3.7s)
time sudo ./nfs-walker-rocks nfs://172.200.203.1/syncengine-demo/bench/mix -q
Success Criteria
ScenarioCurrentTargetMust Not Regressnarrow-deep51.6s<12s—mix3.76s—<5swide-shallow27.9s—<35shuge-dir12.7s—<15s
Additional Notes

The walker requires sudo to run due to libnfs mount permissions (root squash)
RocksDB output adds overhead but is required for the tool's functionality
16 workers is the default; narrow-deep won't benefit from more workers with current design
VAST Data NFS servers handle parallel requests well, so the fix should leverage parallelism where possible


Start by cloning the repo, profiling narrow-deep, then implement the hybrid BFS/DFS approach in Option 1.
We already have the repo cloned so skip that.  
