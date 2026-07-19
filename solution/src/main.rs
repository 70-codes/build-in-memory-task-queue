use in_memory_task_queue::{EnqueueOptions, TaskQueue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn stamp() -> String {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let secs = d.as_secs();
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        (secs / 3600) % 24,
        (secs / 60) % 60,
        secs % 60,
        d.subsec_millis()
    )
}
fn log(msg: impl AsRef<str>) {
    println!("[{}] {}", stamp(), msg.as_ref());
}

fn main() {
    log("Requirement 1: enqueue a task and run it");
    let queue = TaskQueue::new(3);
    queue
        .enqueue(Box::new(|| {
            log("  handler ran with its payload");
            Ok(())
        }))
        .unwrap();
    thread::sleep(Duration::from_millis(200));
    log("  done\n");

    log("Requirement 2: concurrency limit (concurrency=2, 5 tasks ~1s each)");
    let queue = TaskQueue::new(2);
    let running = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));
    for i in 0..5 {
        let running = Arc::clone(&running);
        let max_seen = Arc::clone(&max_seen);
        queue
            .enqueue(Box::new(move || {
                let current = running.fetch_add(1, Ordering::SeqCst) + 1;
                let mut max = max_seen.load(Ordering::SeqCst);
                while current > max {
                    match max_seen.compare_exchange(
                        max,
                        current,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => break,
                        Err(m) => max = m,
                    }
                }
                log(format!("  task {} started (concurrent: {})", i, current));
                thread::sleep(Duration::from_secs(1));
                running.fetch_sub(1, Ordering::SeqCst);
                log(format!("  task {} finished", i));
                Ok(())
            }))
            .unwrap();
    }
    thread::sleep(Duration::from_secs(4));
    log(format!(
        "  max concurrent seen: {} (expected 2)",
        max_seen.load(Ordering::SeqCst)
    ));
    assert_eq!(max_seen.load(Ordering::SeqCst), 2);
    log("  done\n");

    log("Requirement 3: delayed execution (3s delay)");
    let queue = TaskQueue::new(1);
    log(format!("  enqueued at {}", stamp()));
    queue
        .enqueue_with(
            Box::new(|| {
                log("  delayed task started running");
                Ok(())
            }),
            EnqueueOptions {
                delay: Duration::from_secs(3),
                ..Default::default()
            },
        )
        .unwrap();
    thread::sleep(Duration::from_secs(4));
    log("  done\n");

    log("Requirement 4: retry with backoff (max_retries=3, backoff=1s)");
    let queue = TaskQueue::new(1);
    let attempt = Arc::new(AtomicUsize::new(0));
    let ac = Arc::clone(&attempt);
    queue
        .enqueue_with(
            Box::new(move || {
                let a = ac.fetch_add(1, Ordering::SeqCst) + 1;
                log(format!("  attempt {}", a));
                if a < 3 {
                    Err("simulated failure".to_string())
                } else {
                    log("  -> success");
                    Ok(())
                }
            }),
            EnqueueOptions {
                max_retries: 3,
                backoff: Duration::from_secs(1),
                ..Default::default()
            },
        )
        .unwrap();
    thread::sleep(Duration::from_secs(5));
    log("  done\n");

    log("Requirement 5: dead letter queue (max_retries=2, always fails)");
    let queue = TaskQueue::new(1);
    queue
        .enqueue_with(
            Box::new(|| {
                log("  task running and failing");
                Err("permanent failure".to_string())
            }),
            EnqueueOptions {
                max_retries: 2,
                backoff: Duration::from_millis(100),
                ..Default::default()
            },
        )
        .unwrap();
    thread::sleep(Duration::from_secs(1));
    let dlq = queue.get_dead_letters();
    log(format!("  dead letters: {:?}", dlq));
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].attempts, 3);
    log("  done\n");

    log("Requirement 6: graceful shutdown (slow task ~2s)");
    let queue = TaskQueue::new(1);
    queue
        .enqueue(Box::new(|| {
            log("  slow task started");
            thread::sleep(Duration::from_secs(2));
            log("  slow task finished");
            Ok(())
        }))
        .unwrap();
    thread::sleep(Duration::from_millis(100));
    log("  calling shutdown...");
    queue.shutdown();
    log("  shutdown complete");
    let result = queue.enqueue(Box::new(|| Ok(())));
    assert!(result.is_err());
    log("  post-shutdown enqueue rejected\n");

    log("Requirement 7: concurrent enqueue safety (20 tasks from multiple threads)");
    let queue = TaskQueue::new(3);
    let completed = Arc::new(AtomicUsize::new(0));
    thread::scope(|scope| {
        for t in 0..4 {
            let queue_ref = &queue;
            let completed = Arc::clone(&completed);
            scope.spawn(move || {
                for _i in 0..5 {
                    let completed = Arc::clone(&completed);
                    queue_ref
                        .enqueue(Box::new(move || {
                            completed.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        }))
                        .unwrap();
                }
                log(format!("  thread {} enqueued 5 tasks", t));
            });
        }
    });
    thread::sleep(Duration::from_millis(500));
    let total = completed.load(Ordering::SeqCst);
    log(format!("  total completed: {} (expected 20)", total));
    assert_eq!(total, 20);
    log("  done\n");
    log("All requirements passed.");
}
