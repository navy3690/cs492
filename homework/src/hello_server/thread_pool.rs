//! Thread pool that joins all thread when dropped.

#![allow(clippy::mutex_atomic)]

// NOTE: Crossbeam channels are MPMC, which means that you don't need to wrap the receiver in
// Arc<Mutex<..>>. Just clone the receiver and give it to each worker thread.
use crossbeam_channel::{unbounded, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

struct Job(Box<dyn FnOnce() + Send + 'static>);

#[derive(Debug)]
struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

impl Drop for Worker {
    /// When dropped, the thread's `JoinHandle` must be `join`ed.  If the worker panics, then this
    /// function should panic too.  NOTE: that the thread is detached if not `join`ed explicitly.
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

/// Internal data structure for tracking the current job status. This is shared by the worker
/// closures via `Arc` so that the workers can report to the pool that it started/finished a job.
#[derive(Debug, Default)]
struct ThreadPoolInner {
    job_count: Mutex<usize>,
    empty_condvar: Condvar,
}

impl ThreadPoolInner {
    /// Increment the job count.
    fn start_job(&self) {
        *self.job_count.lock().unwrap() += 1;
    }

    /// Decrement the job count.
    fn finish_job(&self) {
        let mut v = self.job_count.lock().unwrap();
        *v -= 1;
        if *v == 0 {
            self.empty_condvar.notify_one();
        }
    }

    /// Wait until the job count becomes 0.
    ///
    /// NOTE: We can optimize this function by adding another field to `ThreadPoolInner`, but let's
    /// not care about that in this homework.
    fn wait_empty(&self) {
        let mut v = self.job_count.lock().unwrap();

        while *v != 0 {
            v = self.empty_condvar.wait(v).unwrap();
        }
    }
}

/// Thread pool.
#[derive(Debug)]
pub struct ThreadPool {
    workers: Vec<Worker>,
    job_sender: Option<Sender<Job>>,
    pool_inner: Arc<ThreadPoolInner>,
}

impl ThreadPool {
    /// Create a new ThreadPool with `size` threads. Panics if the size is 0.
    pub fn new(size: usize) -> Self {
        assert!(size > 0);
        // 스레드들을 생성하고 백터 내에 보관
        let (sender, receiver) = unbounded();

        let mut workers = Vec::with_capacity(size);

        let thread_pool_inner = ThreadPoolInner {
            job_count: Mutex::new(0),
            empty_condvar: Condvar::new(),
        };
        let pool_inner = Arc::new(thread_pool_inner);
        let pool = Arc::clone(&pool_inner);

        for id in 0..size {
            let r = receiver.clone();
            let p = Arc::clone(&pool);
            let thread = thread::spawn(move || loop {
                let job = r.recv();
                match job {
                    Ok(Job(job)) => {
                        job();
                    }
                    Err(_) => break,
                }
                p.finish_job();
            });

            workers.push(Worker {
                id,
                thread: Some(thread),
            });

            // workers.push(Worker::new(id, &receiver.clone()));
        }
        let job_sender = Some(sender);

        ThreadPool {
            workers,
            job_sender,
            pool_inner,
        }
    }

    /// Execute a new job in the thread pool.
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool_inner.start_job();
        let job = Job(Box::new(f));

        let x = &self.job_sender;

        if let Some(sender) = x {
            sender.send(job).unwrap();
        }
    }

    /// Block the current thread until all jobs in the pool have been executed.  NOTE: This method
    /// has nothing to do with `JoinHandle::join`.
    pub fn join(&self) {
        self.pool_inner.wait_empty();
    }
}

impl Drop for ThreadPool {
    /// When dropped, all worker threads' `JoinHandle` must be `join`ed. If the thread panicked,
    /// then this function should panic too.
    fn drop(&mut self) {
        for _ in &self.workers {
            drop(self.job_sender.take());
            //take() none 넣어주고, content 가져오기 => 소유권 가져오기
        }
    }
}

#[cfg(test)]
mod test {
    use super::ThreadPool;
    use crossbeam_channel::bounded;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread::sleep;
    use std::time::Duration;

    const NUM_THREADS: usize = 4;
    const NUM_JOBS: usize = 1024;

    #[test]
    fn thread_pool_parallel() {
        let pool = ThreadPool::new(NUM_THREADS);
        let barrier = Arc::new(Barrier::new(NUM_THREADS));
        let (done_sender, done_receiver) = bounded(NUM_THREADS);
        for _ in 0..NUM_THREADS {
            let barrier = barrier.clone();
            let done_sender = done_sender.clone();
            pool.execute(move || {
                barrier.wait();
                done_sender.send(()).unwrap();
            });
        }
        for _ in 0..NUM_THREADS {
            done_receiver.recv_timeout(Duration::from_secs(3)).unwrap();
        }
    }

    // Run jobs that take NUM_JOBS milliseconds as a whole.
    fn run_jobs(pool: &ThreadPool, counter: &Arc<AtomicUsize>) {
        for _ in 0..NUM_JOBS {
            let counter = counter.clone();
            pool.execute(move || {
                sleep(Duration::from_millis(NUM_THREADS as u64));
                counter.fetch_add(1, Ordering::Relaxed);
            });
        }
    }

    /// `join` blocks until all jobs are finished.
    #[test]
    fn thread_pool_join_block() {
        let pool = ThreadPool::new(NUM_THREADS);
        let counter = Arc::new(AtomicUsize::new(0));
        run_jobs(&pool, &counter);
        pool.join();
        assert_eq!(counter.load(Ordering::Relaxed), NUM_JOBS);
    }

    /// `drop` blocks until all jobs are finished.
    #[test]
    fn thread_pool_drop_block() {
        let pool = ThreadPool::new(NUM_THREADS);
        let counter = Arc::new(AtomicUsize::new(0));
        run_jobs(&pool, &counter);
        drop(pool);
        assert_eq!(counter.load(Ordering::Relaxed), NUM_JOBS);
    }

    /// This indirectly tests if the worker threads' `JoinHandle`s are joined when the pool is
    /// dropped.
    #[test]
    #[should_panic]
    fn thread_pool_drop_propagate_panic() {
        let pool = ThreadPool::new(NUM_THREADS);
        pool.execute(move || {
            panic!();
        });
    }
}
