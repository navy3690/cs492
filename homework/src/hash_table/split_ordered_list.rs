//! Split-ordered linked list.

use core::mem;
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{Guard, Owned, Shared, Atomic};
use lockfree::list::{Cursor, List, Node};

use super::growable_array::GrowableArray;
use crate::map::NonblockingMap;

/// Lock-free map from `usize` in range [0, 2^63-1] to `V`.
///
/// NOTE: We don't care about hashing in this homework for simplicity.
#[derive(Debug)]
pub struct SplitOrderedList<V> {
    /// Lock-free list sorted by recursive-split order. Use `None` sentinel node value.
    list: List<usize, Option<V>>,
    /// array of pointers to the buckets
    buckets: GrowableArray<Node<usize, Option<V>>>,
    /// number of buckets
    size: AtomicUsize,
    /// number of items
    count: AtomicUsize,
}

impl<V> Default for SplitOrderedList<V> {
    fn default() -> Self {
        Self {
            list: List::new(),
            buckets: GrowableArray::new(),
            size: AtomicUsize::new(2),
            count: AtomicUsize::new(0),
        }
    }
}

impl<V> SplitOrderedList<V> {
    /// `size` is doubled when `count > size * LOAD_FACTOR`.
    const LOAD_FACTOR: usize = 2;

    /// Creates a new split ordered list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a cursor and moves it to the bucket for the given index.  If the bucket doesn't
    /// exist, recursively initializes the buckets.
    fn lookup_bucket<'s>(&'s self, index: usize, guard: &'s Guard) -> Cursor<'s, usize, Option<V>> {
        
        // bucket list에서 pointer 받아오기
        // pointer가 sentinel_key 가르키기
        // sentinel_key를 None value 설정해서 list에 삽입
        // sentinel_key가 없을 때
        // sentinel_key는 있는데 아직 insert 안했을 때
        self.initialize_bucket(index, self.size.load(Ordering::Acquire), guard)
    }

    fn get_parent<'s>(index: usize, size: usize) -> usize {
        let mut parent = size;
        loop{
            if parent > index {
                parent = parent >> 1;
            }
            else { break; }
        }
        parent = index - parent;
        parent
    }

    fn initialize_bucket<'s>(&'s self, index: usize, size: usize, guard: &'s Guard) -> Cursor<'s, usize, Option<V>>{   
        unsafe {
            let bucket_ptr = self.buckets.get(index,guard);
            let mut cursor;
            let parent = Self::get_parent(index, size);
            let none_value: Option<V> = None;
            let sentinel_index = index.reverse_bits();
            let mut sentinel_node = Owned::new(Node::new(sentinel_index, none_value));
            
            loop {
                let mut found;
                loop{
                    let sentinel_ptr = bucket_ptr.load(Ordering::Acquire,guard);
                    if !sentinel_ptr.is_null(){
                        cursor =  Cursor::from_raw(&Atomic::null(),sentinel_ptr.deref());
                        found = true;
                        break;
                    }
                    if index!=0 {
                        cursor = self.initialize_bucket(parent, size, guard);
                    }
                    else{
                        cursor = self.list.head(guard);
                    }
                    if let Ok(b) = cursor.find_harris_michael(&sentinel_index, guard){
                        found = b;
                        break;
                    }
                }
                if found {
                    break;
                }
                match cursor.insert(sentinel_node, guard){
                    Err(n) => sentinel_node = n,
                    Ok(()) => {
                        bucket_ptr.store(cursor.curr(), Ordering::Release);
                        break;
                    }
                }
            }
            cursor
        }
    }

    /// Moves the bucket cursor returned from `lookup_bucket` to the position of the given key.
    /// Returns `(size, found, cursor)`
    fn find<'s>(
        &'s self,
        key: &usize,
        guard: &'s Guard,
    ) -> (usize, bool, Cursor<'s, usize, Option<V>>) {
        let bucket_size = self.size.load(Ordering::Acquire);
        let bucket_index = (*key) % bucket_size;
        let mask:usize = 1 << 63;
        let new_index = ((*key)|mask).reverse_bits();
        let mut cursor;
        let mut found = false;
        loop{
            cursor = self.lookup_bucket(bucket_index,guard);
            if let Ok(b) = cursor.find_harris_michael(&new_index, guard){
                found = b;
                break;
            }
        }
        (bucket_size, found, cursor)
    }

    fn assert_valid_key(key: usize) {
        assert!(key.leading_zeros() != 0);
    }
}

impl<V> NonblockingMap<usize, V> for SplitOrderedList<V> {
    fn lookup<'a>(&'a self, key: &usize, guard: &'a Guard) -> Option<&'a V> {
        Self::assert_valid_key(*key);
        let (size,found,cursor) = self.find(key, guard);
        let none_value: Option<&V> = None;
        
        
        if found {
            let value = cursor.lookup();
            match value {
                Some(v) => return v.as_ref(),
                None => unreachable!()
            }
        }
        else { none_value }
    }

    fn insert(&self, key: &usize, value: V, guard: &Guard) -> Result<(), V> {
        Self::assert_valid_key(*key);
        let mask:usize = 1 << 63;
        let new_key = ((*key)|mask).reverse_bits();
        let v:Option<V> = Some(value);
        let mut new_node = Owned::new(Node::new(new_key,v));
        loop{
            let (size,found,mut cursor) = self.find(key, guard);
            if found {
                let error_value = new_node.into_box().into_value();
                match error_value {
                    Some(t) => {
                        return Err(t)
                    },
                    None => unreachable!()
                }
            }
            match cursor.insert(new_node, guard){
                Err(n) => new_node = n,
                Ok(()) => {
                    let old_count = self.count.fetch_add(1, Ordering::Release);
                    if (old_count + 1) > (size * 2){
                        self.size.compare_and_swap(size, size * 2,Ordering::AcqRel);
                    }
                    return Ok(())
                }
            }
        }
        // todo!()
    }

    fn delete<'a>(&'a self, key: &usize, guard: &'a Guard) -> Result<&'a V, ()> {
        Self::assert_valid_key(*key);
        loop{
            let (size,found,cursor) = self.find(key, guard);
            if !found {
                return Err(())
            }
            match cursor.delete(guard){
                Err(()) => continue,
                Ok(value) => {
                    self.count.fetch_sub(1, Ordering::Release);
                    match value {
                        Some(v) => return Ok(v),
                        None => unreachable!()
                    }
                }
            }
        }
    }
}
