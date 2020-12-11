//! Growable array.

use core::fmt::Debug;
use core::marker::PhantomData;
use core::mem;
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{AtomicUsize, Ordering};
use std::ptr::null;
use crossbeam_epoch::{unprotected, Atomic, Guard, Owned, Pointer, Shared};
use mem::size_of;

/// Growable array of `Atomic<T>`.
///
/// This is more complete version of the dynamic sized array from the paper. In the paper, the
/// segment table is an array of arrays (segments) of pointers to the elements. In this
/// implementation, a segment contains the pointers to the elements **or other segments**. In other
/// words, it is a tree that has segments as internal nodes.
///
/// # Example run
///
/// Suppose `SEGMENT_LOGSIZE = 3` (segment size 8).
///
/// When a new `GrowableArray` is created, `root` is initialized with `Atomic::null()`.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
/// ```
///
/// When you store element `cat` at the index `0b001`, it first initializes a segment.
///
/// ```text
///
///                          +----+
///                          |root|
///                          +----+
///                            | height: 1
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                           |
///                                           v
///                                         +---+
///                                         |cat|
///                                         +---+
/// ```
///
/// When you store `fox` at `0b111011`, it is clear that there is no room for indices larger than
/// `0b111`. So it first allocates another segment for upper 3 bits and moves the previous root
/// segment (`0b000XXX` segment) under the `0b000XXX` branch of the the newly allocated segment.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                               |
///                                               v
///                                      +---+---+---+---+---+---+---+---+
///                                      |111|110|101|100|011|010|001|000|
///                                      +---+---+---+---+---+---+---+---+
///                                                                |
///                                                                v
///                                                              +---+
///                                                              |cat|
///                                                              +---+
/// ```
///
/// And then, it allocates another segment for `0b111XXX` indices.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                                            |
///                   v                                            v
///                 +---+                                        +---+
///                 |fox|                                        |cat|
///                 +---+                                        +---+
/// ```
///
/// Finally, when you store `owl` at `0b000110`, it traverses through the `0b000XXX` branch of the
/// level-1 segment and arrives at its 0b110` leaf.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                        |                   |
///                   v                        v                   v
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// When the array is dropped, only the segments are dropped and the **elements must not be
/// dropped/deallocated**.
///
/// ```test
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// Instead, it should be handled by the container that the elements actually belong to. For
/// example in `SplitOrderedList`, destruction of elements are handled by `List`.
///
#[derive(Debug)]
pub struct GrowableArray<T> {
    root: Atomic<Segment>,
    _marker: PhantomData<T>,
}

const SEGMENT_LOGSIZE: usize = 10;

struct Segment {
    /// `AtomicUsize` here means `Atomic<T>` or `Atomic<Segment>`.
    inner: [AtomicUsize; 1 << SEGMENT_LOGSIZE],
}

impl Segment {
    fn new() -> Self {
        Self {
            inner: unsafe { mem::zeroed() },
        }
    }
}

impl Deref for Segment {
    type Target = [AtomicUsize; 1 << SEGMENT_LOGSIZE];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Segment {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Debug for Segment {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Segment")
    }
}

impl<T> Drop for GrowableArray<T> {
    /// Deallocate segments, but not the individual elements.
    fn drop(&mut self) {
        
        unsafe{
            let seg = self.root.load(Ordering::Acquire,unprotected());
            if seg.is_null()==false{
                drop_seg(seg,seg.tag());
            }
        }
        
        fn drop_seg(seg: Shared<Segment>, height: usize){
            let mut index = 0usize;
            loop {
                unsafe{ 
                    let index_ptr = &*(seg.deref()).get_unchecked(index); 
                    let next_seg: Shared<Segment> = Shared::from_usize(index_ptr.load(Ordering::Acquire)); 
                    if next_seg.is_null()==false {
                        if height>1 {
                            drop_seg(next_seg,height-1)
                        } 
                    }
                }
                if index==1023  {break;}
                else { index+=1; }
            }
            unsafe{
                seg.into_owned();
            }
        }

    }
}

impl<T> Default for GrowableArray<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> GrowableArray<T> {
    /// Create a new growable array.
    pub fn new() -> Self {
        Self {
            root: Atomic::null(),
            _marker: PhantomData,
        }
    }

    /// Returns the reference to the `Atomic` pointer at `index`. Allocates new segments if
    /// necessary.
    pub fn get(&self, mut index: usize, guard: &Guard) -> &Atomic<T> {
        
        let bit_num = 64-index.leading_zeros();
        let new_ptr = Owned::new(Segment::new());
        let r = self.root.compare_and_set(Shared::null(), new_ptr.with_tag(1), Ordering::AcqRel, guard);

        let mut root = match r {
            Err(e) => e.current,
            Ok(t) => t
        };

        let mut height = root.tag();
        let bit_height;
        if (bit_num%10)==0 {
            bit_height = (bit_num/10) as usize;
        }
        else {
            bit_height = ((bit_num/10)+1) as usize;
        }
        
        loop {
            if height<bit_height{
                let next = Owned::new(Segment::new()); 
                unsafe {
                    let index_zero = &*next.get_unchecked(usize::MIN);
                    index_zero.store(root.into_usize(), Ordering::Release);
                    let result = self.root.compare_and_set(root, next.with_tag(height+1), Ordering::AcqRel, guard);
                    match result {
                        Err(e) => root = e.current,
                        Ok(t) => root = t
                    }
                }
                height = root.tag();
            }
            else { break; }
        }
    
        let mut curr_seg = root;
        let mut currIndex = index;
        let mut root_height = root.tag();
        loop{
            let two:usize = 2;
            let max_bit = two.pow(10 * root_height as u32) - 1;
            let mut new_index = currIndex & max_bit;
            new_index = new_index >> (10 * (root_height -1));
            if(root_height>1){
                unsafe{
                    let seg_index = (curr_seg.deref()).get_unchecked(new_index);
                    let val = Shared::into_usize(Owned::new(Segment::new()).into_shared(guard));
                    let usize_zero = seg_index.compare_and_swap(0, val, Ordering::AcqRel);
                    match usize_zero {
                        0 => curr_seg = Shared::from_usize(val),
                        _ =>{
                            let x:Owned<Segment> = Shared::from_usize(val).into_owned();
                            curr_seg = Shared::from_usize(usize_zero)
                        }   
                    }
                }
                root_height-=1;
            }
            else{
                unsafe{ 
                    let seg = curr_seg.deref();
                    return &*(seg.get_unchecked(new_index) as *const _ as *const Atomic<T>)
                }
            }
        }
    }
}
