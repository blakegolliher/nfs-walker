//! Benchmarks for nfs-walker
//!
//! Run with: cargo bench

use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn benchmark_queue_operations(c: &mut Criterion) {
    use nfs_walker::walker::queue::{DirTask, WorkQueue};

    c.bench_function("queue_send_recv", |b| {
        let queue = WorkQueue::new(10000);
        let sender = queue.sender();
        let receiver = queue.receiver();

        b.iter(|| {
            let task = DirTask::new("/test/path".into(), Some(1), 5);
            sender.send(task).unwrap();
            let received = receiver.try_recv().unwrap();
            black_box(received);
        })
    });
}

fn benchmark_db_entry_creation(c: &mut Criterion) {
    use nfs_walker::nfs::types::{DbEntry, EntryType, NfsDirEntry, NfsStat};

    c.bench_function("db_entry_from_nfs", |b| {
        let nfs_entry = NfsDirEntry {
            name: "test_file.txt".into(),
            entry_type: EntryType::File,
            stat: Some(NfsStat {
                size: 1024,
                inode: 12345,
                nlink: 1,
                uid: 1000,
                gid: 1000,
                mode: 0o100644,
                atime: Some(1234567890),
                mtime: Some(1234567890),
                ctime: Some(1234567890),
                blksize: 4096,
                blocks: 8,
            }),
            inode: 12345,
        };

        b.iter(|| {
            let entry = DbEntry::from_nfs_entry(
                &nfs_entry,
                Some(1),
                "/parent/dir",
                3,
            );
            black_box(entry);
        })
    });
}

criterion_group!(benches, benchmark_queue_operations, benchmark_db_entry_creation);
criterion_main!(benches);
