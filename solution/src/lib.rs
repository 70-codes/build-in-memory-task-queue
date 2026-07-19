use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub type Job = Box<dyn Fn() -> Result<(), String> + Send>;

pub struct EnqueueOptions {
    pub delay: Duration,
    pub max_retries: u32,
    pub backoff: Duration,
}
impl Default for EnqueueOptions {
    fn default() -> Self {
        EnqueueOptions { delay: Duration::ZERO, max_retries: 0, backoff: Duration::from_secs(1) }
    }
}

#[derive(Debug)]
pub struct Rejected;

#[derive(Debug, Clone)]
pub struct DeadLetter {
    pub id: u64,
    pub error: String,
    pub attempts: u32,
}

struct Task {
    id: u64,
    job: Job,
    attempt: u32,
    max_retries: u32,
    backoff: Duration,
    ready_at: Instant,
}

struct Scheduled(Task);
impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool { self.0.ready_at == other.0.ready_at && self.0.id == other.0.id }
}
impl Eq for Scheduled {}
impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.ready_at.cmp(&self.0.ready_at).then_with(|| other.0.id.cmp(&self.0.id))
    }
}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}

struct Shared {
    heap: BinaryHeap<Scheduled>,
    dead_letters: Vec<DeadLetter>,
    shutting_down: bool,
    next_id: u64,
}
struct Inner { state: Mutex<Shared>, cv: Condvar }

pub struct TaskQueue {
    inner: Arc<Inner>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl TaskQueue {
    pub fn new(concurrency: usize) -> Self {
        let inner = Arc::new(Inner {
            state: Mutex::new(Shared { heap: BinaryHeap::new(), dead_letters: Vec::new(), shutting_down: false, next_id: 1 }),
            cv: Condvar::new(),
        });
        let mut workers = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let inner = Arc::clone(&inner);
            workers.push(thread::spawn(move || worker_loop(inner)));
        }
        TaskQueue { inner, workers: Mutex::new(workers) }
    }
    pub fn enqueue(&self, job: Job) -> Result<u64, Rejected> { self.enqueue_with(job, EnqueueOptions::default()) }
    pub fn enqueue_with(&self, job: Job, opts: EnqueueOptions) -> Result<u64, Rejected> {
        let mut s = self.inner.state.lock().unwrap();
        if s.shutting_down { return Err(Rejected); }
        let id = s.next_id; s.next_id += 1;
        let task = Task { id, job, attempt: 0, max_retries: opts.max_retries, backoff: opts.backoff, ready_at: Instant::now() + opts.delay };
        s.heap.push(Scheduled(task));
        drop(s);
        self.inner.cv.notify_one();
        Ok(id)
    }
    pub fn shutdown(&self) {
        let mut s = self.inner.state.lock().unwrap();
        s.shutting_down = true;
        drop(s);
        self.inner.cv.notify_all();
        let mut workers = self.workers.lock().unwrap();
        for w in workers.drain(..) { let _ = w.join(); }
    }
    pub fn get_dead_letters(&self) -> Vec<DeadLetter> {
        self.inner.state.lock().unwrap().dead_letters.clone()
    }
}