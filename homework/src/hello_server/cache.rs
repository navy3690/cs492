//! Thead-safe key/value cache.

use std::collections::hash_map::{Entry, HashMap};
use std::hash::Hash;
use std::sync::{Arc, Mutex, RwLock};

/// Cache that remembers the result for each key.
#[derive(Debug, Default)]
pub struct Cache<K, V> {
    // todo! Build your own cache type.
    inner: RwLock<HashMap<K, Arc<Mutex<Option<V>>>>>,
}

impl<K: Eq + Hash + Clone, V: Clone> Cache<K, V> {
    /// Retrieve the value or insert a new one created by `f`.
    ///
    /// An invocation to this function should not block another invocation with a different key.
    /// For exmaple, if a thread calls `get_or_insert_with(key1, f1)` and another thread calls
    /// `get_or_insert_with(key2, f2)` (`key1≠key2`, `key1,key2∉cache`) concurrently, `f1` and `f2`
    /// should run concurrently.
    ///
    /// On the other hand, since `f` may consume a lot of resource (= money), it's desirable not to
    /// duplicate the work. That is, `f` should be run only once for each key. Specifically, even
    /// for the concurrent invocations of `get_or_insert_with(key, f)`, `f` is called only once.
    pub fn get_or_insert_with<F: FnOnce(K) -> V>(&self, key: K, f: F) -> V {
        // vale가 있다면 key return, 또는 excute
        // let map = self.inner.read().unwrap();
        // let contain = map.get(&key);

        // if let Some(v) = contain {
        //     let r = &*v.lock().unwrap();
        //     r.clone()
        // }
        // else{
        //     drop(map);
        //     let ff = f(key.clone());
        //     let value = Arc::new(Mutex::new(ff.clone()));
        //     let mut map = self.inner.write().unwrap();
        //     map.insert(key.clone(),Arc::clone(&value));
        //     drop(map);
        //     ff
        // }
        let map = self.inner.read().unwrap();
        let contain = map.get(&key);

        if let Some(v) = contain {
            let r = &*v.lock().unwrap();
            match r {
                Some(v) => v.clone(),
                None => unreachable!(),
            }
        } else {
            drop(map);
            let mut map = self.inner.write().unwrap();
            let cont = map.get(&key);
            if let Some(mtx) = cont {
                let r_mutex = mtx.lock().unwrap();
                let ff = match &*r_mutex {
                    Some(v) => v.clone(),
                    None => unreachable!(),
                };
                ff
            } else {
                let x: Option<V> = None;
                let mtx = Arc::new(Mutex::new(x));
                map.insert(key.clone(), mtx.clone());
                let mut r_mutex = mtx.lock().unwrap();
                drop(map);
                let ff = f(key.clone());
                let ff: Option<V> = Some(ff);
                // let mut map = self.inner.write().unwrap();
                if let None = &*r_mutex {
                    // let x = *r_mutex;
                    *r_mutex = ff.clone();
                }
                // map.insert(key.clone(),Arc::clone(&value));
                // drop(map);
                match ff {
                    Some(v) => v.clone(),
                    None => panic!(),
                }
            }

            // drop(map);
            // let map = self.inner.read().unwrap();
            // let vv = map.get(&key);
            // let v = match vv {
            //     Some(v) => Arc::clone(&v),
            //     None => unreachable!()
            // };
            // // drop(map);
            // let ff = f(key.clone());
            // let ff : Option<V> = Some(ff);
            // let value = Arc::new(Mutex::new(ff.clone()));
            // let mut map = self.inner.write().unwrap();
            // map.insert(key.clone(),Arc::clone(&value));
            // drop(map);
            // match ff {
            //     Some(v) => v.clone(),
            //     None => unreachable!()
            // }
        }
        // let map = self.inner.read().unwrap();
        // let contain = map.get(&key);

        // if let Some(v) = contain {
        //     let x = Arc::clone(&v);
        //     let x = &*x.lock().unwrap();
        //     x.clone()
        // }
        // else{
        //     drop(map);
        //     let kk = key.clone();
        //     let k = Mutex::new(key);
        //     // let kk = Arc::new(Mutex::new(key.clone()));
        //     // let k = &*kk.lock().unwrap();
        //     let ff = f(k.lock().unwrap().clone());
        //     let value = ff.clone();
        //     let mut map = self.inner.write().unwrap();
        //     map.insert(kk.clone(),Arc::new(Mutex::new(value)));
        //     drop(map);
        //     ff

        //RwLock은 많은 reader가 읽을 수 있음.
    }
}

#[cfg(test)]
mod test {
    use super::Cache;
    use crossbeam_channel::bounded;
    use crossbeam_utils::thread::scope;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Barrier;
    use std::time::Duration;

    const NUM_THREADS: usize = 8;
    const NUM_KEYS: usize = 128;

    #[test]
    fn cache_no_duplicate_sequential() {
        let cache = Cache::default();
        cache.get_or_insert_with(1, |_| 1);
        cache.get_or_insert_with(2, |_| 2);
        cache.get_or_insert_with(3, |_| 3);
        assert_eq!(cache.get_or_insert_with(1, |_| panic!()), 1);
        assert_eq!(cache.get_or_insert_with(2, |_| panic!()), 2);
        assert_eq!(cache.get_or_insert_with(3, |_| panic!()), 3);
    }

    #[test]
    fn cache_no_duplicate_concurrent() {
        for _ in 0..8 {
            let cache = Cache::default();
            let barrier = Barrier::new(NUM_THREADS);
            // Count the number of times the computation is run.
            let num_compute = AtomicUsize::new(0);
            scope(|s| {
                for _ in 0..NUM_THREADS {
                    s.spawn(|_| {
                        barrier.wait();
                        for key in 0..NUM_KEYS {
                            cache.get_or_insert_with(key, |k| {
                                num_compute.fetch_add(1, Ordering::Relaxed);
                                k
                            });
                        }
                    });
                }
            })
            .unwrap();
            assert_eq!(num_compute.load(Ordering::Relaxed), NUM_KEYS);
        }
    }

    #[test]
    fn cache_no_block_disjoint() {
        let cache = &Cache::default();

        scope(|s| {
            // T1 blocks while inserting 1.
            let (t1_quit_sender, t1_quit_receiver) = bounded(0);
            s.spawn(move |_| {
                cache.get_or_insert_with(1, |k| {
                    t1_quit_receiver.recv().unwrap();
                    k
                });
            });

            // T2 must not be blocked by T1 when inserting 2.
            let (t2_done_sender, t2_done_receiver) = bounded(0);
            s.spawn(move |_| {
                cache.get_or_insert_with(2, |k| k);
                t2_done_sender.send(()).unwrap();
            });

            // If T2 is blocked, then recv will time out.
            t2_done_receiver
                .recv_timeout(Duration::from_secs(3))
                .expect("Inserting a different key should not block");

            t1_quit_sender.send(()).unwrap();
        })
        .unwrap();
    }
}
