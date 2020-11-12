#![allow(clippy::mutex_atomic)]
use std::cmp;
use std::ptr;
use std::sync::{Mutex, MutexGuard};

#[derive(Debug)]
struct Node<T> {
    data: T,
    next: Mutex<*mut Node<T>>,
}

unsafe impl<T> Send for Node<T> {}
unsafe impl<T> Sync for Node<T> {}

/// Concurrent sorted singly linked list using lock-coupling.
#[derive(Debug)]
pub struct OrderedListSet<T> {
    head: Mutex<*mut Node<T>>,
}

unsafe impl<T> Send for OrderedListSet<T> {}
unsafe impl<T> Sync for OrderedListSet<T> {}

// reference to the `next` field of previous node which points to the current node
struct Cursor<'l, T>(MutexGuard<'l, *mut Node<T>>);

impl<T> Node<T> {
    fn new(data: T, next: *mut Self) -> *mut Self {
        Box::into_raw(Box::new(Self {
            data,
            next: Mutex::new(next),
        }))
    }
}

impl<'l, T: Ord> Cursor<'l, T> {
    /// Move the cursor to the position of key in the sorted list. If the key is found in the list,
    /// return `true`.
    fn find(&mut self, key: &T) -> bool {
        unsafe{
            loop {
                let node = *self.0;
                if node.is_null() {
                    break;
                } 
                let data = &(*node).data;
                
                if *key < *data{
                    break;
                }
                else if *key == *data{
                    return true;
                }
                else{
                    let next = (*(*self.0)).next.lock().unwrap();
                    self.0 = next;
                    continue;
                }
                
            }
            return false;
        }
    }
}

impl<T> OrderedListSet<T> {
    /// Creates a new list.
    pub fn new() -> Self {
        Self {
            head: Mutex::new(ptr::null_mut()),
        }
    }
}

impl<T: Ord> OrderedListSet<T> {
    fn find(&self, key: &T) -> (bool, Cursor<T>) {
        let head = self.head.lock().unwrap();
        let mut cursor = Cursor(head);
        let success = cursor.find(key);
        (success, cursor)
    }

    /// Returns `true` if the set contains the key.
    pub fn contains(&self, key: &T) -> bool {
        let head = self.head.lock().unwrap();
        let mut cursor = Cursor(head);
        cursor.find(key)
    }

    /// Insert a key to the set. If the set already has the key, return the provided key in `Err`.
    pub fn insert(&self, key: T) -> Result<(), T> {
        let head = self.head.lock().unwrap();
        let mut cursor = Cursor(head);
        if cursor.find(&key) {
            Err(key)
        }
        else{
            let next = *cursor.0;
            let new = Node::new(key,next);
            *cursor.0 = new;
            Ok(())
        }
    }

    /// Remove the key from the set and return it.
    pub fn remove(&self, key: &T) -> Result<T, ()> {
        unsafe {
            let head = self.head.lock().unwrap();
            let mut cursor = Cursor(head);
            if cursor.find(key) {
                let remove = Box::from_raw(*cursor.0);
                let data = remove.data;
                let next = (*remove).next.lock().unwrap();
                *cursor.0 = *next;
                Ok(data)
            }
            else{
                Err(())
            }
        }  
    }
}

#[derive(Debug)]
pub struct Iter<'l, T>(Option<MutexGuard<'l, *mut Node<T>>>);

impl<T> OrderedListSet<T> {
    /// An iterator visiting all elements.
    pub fn iter(&self) -> Iter<T> {
        Iter(Some(self.head.lock().unwrap()))
    }
}

impl<'l, T> Iterator for Iter<'l, T> {
    type Item = &'l T;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            match &self.0{
                None => {
                    None
                },
                Some(m) => {
                    let node = **m;
                    if node.is_null() {
                        self.0 = None;
                        None
                    }
                    else{
                        let data = &(*node).data;
                        let next = (*node).next.lock().unwrap();
                        self.0 = Some(next);
                        Some(data)
                    }
                }
            }
        }
    }
}

impl<T> Drop for OrderedListSet<T> {
    fn drop(&mut self) {
        unsafe {
            let mut head = *self.head.get_mut().unwrap();
            if head.is_null(){
                return;
            }
            loop{
                let next = Box::from_raw(head);
                let next = (*next).next;
                let x = next.into_inner();
                match x {
                    Ok(n) => {
                        if n.is_null() {
                            break;
                        }
                        head = n;
                    }
                    _ => break
                }
            }
        }
    }
}

impl<T> Default for OrderedListSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
