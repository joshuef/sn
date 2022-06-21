// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use dashmap::DashMap;
use priority_queue::PriorityQueue;
use std::{
    cell::RefCell,
    rc::Rc,
    sync::atomic::{AtomicU16, Ordering},
};
use xor_name::XorName;

type Priority = u16;

/// An lru cache with a neat implementation to evict least recently used
/// element, by using a priority queue.
///
/// Implemented as a map of data and a priority queue. The cache stores the items inside a [`DashMap`]
/// to be able to retrieve them quickly. The key is kept in a [`PriorityQueue`], and every time it is accessed,
/// the priority is changed to a lower number.
///
/// At an insert of a new value, when the cache is full, the priority queue will simply be popped, and the least
/// recently used value will have the largest number, so it will be at the top of the queue.
#[derive(Clone, Debug)]
pub(crate) struct LruCache<T> {
    data: DashMap<XorName, Rc<T>>,
    queue: Rc<RefCell<PriorityQueue<XorName, Priority>>>,
    size: u16,
    start: Rc<AtomicU16>,
}

impl<T> LruCache<T> {
    pub(crate) fn new(size: u16) -> Self {
        Self {
            data: DashMap::new(),
            queue: Rc::new(RefCell::new(PriorityQueue::new())),
            size,
            start: Rc::new(AtomicU16::new(u16::MAX)),
        }
    }

    pub(crate) fn insert(&self, key: &XorName, val: Rc<T>) {
        if self.data.contains_key(key) {
            return;
        }

        let _ = self.data.insert(*key, val);
        {
            let mut prio = self.priority();
            if prio == 0 {
                // empty the cache when we overflow
                self.queue.borrow_mut().clear();
                self.data.clear();
                prio = self.start.fetch_sub(1, Ordering::SeqCst)
            }

            let _ = self.queue.borrow_mut().push(*key, prio);
        }

        let len = { self.queue.borrow().len() as u16 };
        if len > self.size {
            let mut write = self.queue.borrow_mut();
            if let Some((evicted, _)) = write.pop() {
                let _ = self.data.remove(&evicted);
            }
        }
    }

    pub(crate) fn get(&self, key: &XorName) -> Option<Rc<T>> {
        let exists = {
            let read_only = self.queue.borrow();
            read_only.get(key).is_some()
        };
        if exists {
            let mut prio = self.priority();
            if prio == 0 {
                // empty the cache when we overflow
                self.queue.borrow_mut().clear();
                self.data.clear();
                prio = self.start.fetch_sub(1, Ordering::SeqCst)
            }

            let _ = self.queue.borrow_mut().change_priority(key, prio);
        }
        self.data.get(key).as_deref().cloned()
    }

    pub(crate) fn remove(&self, key: &XorName) {
        let _ = self.queue.borrow_mut().remove(key);
        let _ = self.data.remove(key);
    }

    /// returns current priority
    fn priority(&self) -> Priority {
        self.start.fetch_sub(1, Ordering::SeqCst)
    }
}

#[cfg(test)]
mod test {
    use super::LruCache;

    use std::sync::Rc;

    #[tokio::test]
    async fn test_basic() {
        let cache = LruCache::new(3);

        let key_1 = &xor_name::rand::random();
        let key_2 = &xor_name::rand::random();
        let key_3 = &xor_name::rand::random();
        cache.insert(key_1, Rc::new("Strawberries"));
        cache.insert(key_2, Rc::new("Bananas"));
        cache.insert(key_3, Rc::new("Peaches"));

        let result_string = format!("{:?}", cache.get(key_2));
        let expected_string = format!("{:?}", Some("Bananas"));

        assert_eq!(result_string, expected_string);
    }

    #[tokio::test]
    async fn test_lru() {
        let cache = LruCache::new(3);

        let key_1 = &xor_name::rand::random();
        let key_2 = &xor_name::rand::random();
        let key_3 = &xor_name::rand::random();
        let key_4 = &xor_name::rand::random();
        cache.insert(key_1, Rc::new("Strawberries"));
        cache.insert(key_2, Rc::new("Bananas"));
        cache.insert(key_3, Rc::new("Peaches"));
        cache.insert(key_4, Rc::new("Blueberries"));

        let result_string = format!("{:?}", cache.get(key_1));
        let expected_string = format!("{:?}", None::<String>);

        assert_eq!(result_string, expected_string);
    }

    #[tokio::test]
    async fn test_remove() {
        let cache = LruCache::new(3);

        let key_1 = &xor_name::rand::random();
        let key_2 = &xor_name::rand::random();
        let key_3 = &xor_name::rand::random();
        cache.insert(key_1, Rc::new("Strawberries"));
        cache.insert(key_2, Rc::new("Bananas"));
        cache.insert(key_3, Rc::new("Peaches"));

        let result_string = format!("{:?}", cache.get(key_2));
        let expected_string = format!("{:?}", Some("Bananas"));

        assert_eq!(result_string, expected_string);

        cache.remove(key_2);

        let result_string = format!("{:?}", cache.get(key_2));
        let expected_string = format!("{:?}", None::<String>);

        assert_eq!(result_string, expected_string);
    }
}
