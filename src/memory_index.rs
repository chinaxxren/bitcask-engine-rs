use crate::bitcask::{ByteOffset, ByteSize, FileId, Key};
use std::collections::btree_map::{BTreeMap, IntoIter};

#[derive(Debug, Clone, PartialEq, Eq)]
/// 内存索引项结构体
///
/// 该结构体用于表示内存中数据的索引项，包含了文件ID、值的偏移量和值的大小
/// 主要用于快速定位和访问内存中的数据
pub(crate) struct MemIndexEntry {
    /// 文件ID，用于标识数据所属的文件
    pub(crate) file_id: FileId,
    /// 值的偏移量，表示数据在内存中的起始位置偏移量
    pub(crate) value_offset: ByteOffset,
    /// 值的大小，表示数据在内存中占用的字节数
    pub(crate) value_size: ByteSize,
}

impl MemIndexEntry {
    /// 检查当前条目是否为墓碑条目。
    ///
    /// 墓碑条目用于标记一个条目已被删除。在某些数据库或存储系统中，当一个条目被删除后，
    /// 其位置可能仍需要被保留或标记，以避免数据的混乱或冲突。这个方法通过检查条目的值
    /// 大小是否为0来判断条目是否为墓碑条目。如果值大小为0，则表示该条目是一个墓碑条目。
    ///
    /// # 返回
    /// * `bool` - 如果当前条目是墓碑条目，则返回`true`；否则返回`false`。
    pub(crate) fn is_tombstone(&self) -> bool {
        self.value_size == 0
    }
}

/// 内存索引结构体，用于高效地在内存中索引和检索数据。
/// 使用BTreeMap来存储键值对，以保持键的有序性，从而提高查找效率。
///
/// # Fields
/// - `map`: BTreeMap<Key, MemIndexEntry> 类型，用于存储索引项。
///   `Key` 是索引的键，`MemIndexEntry` 是每个键对应的索引项，包含键对应的值以及相关元数据。
#[derive(Debug, Clone)]
pub(crate) struct MemIndexStorage {
    map: BTreeMap<Key, MemIndexEntry>,
}

impl MemIndexStorage {
    /// 创建一个新的、空的`BTreeMap`实例。
    ///
    /// ## Returns
    /// 返回一个新的`Self`类型实例，其中`map`字段是一个空的`BTreeMap`。
    pub(crate) fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }
    /// 根据给定的键获取内存索引项的引用。
    ///
    /// ## 参数
    /// - `key`: 要查找的键引用。
    ///
    /// ## 返回
    /// - `Option<&MemIndexEntry>`: 如果找到键，则返回其对应内存索引项的引用；否则返回`None`。
    ///
    /// 此方法提供了一种通过键访问内存索引项的简便方式，主要用于在内存中快速查找数据。
    pub(crate) fn get(&self, key: &Key) -> Option<&MemIndexEntry> {
        self.map.get(key)
    }
    /// 将给定的键值对插入到内存索引中。
    ///
    /// # 参数
    /// - `key`: 要插入的键。
    /// - `entry`: 要插入的条目。
    ///
    /// # 返回值
    /// 如果插入的键已存在于索引中，则返回该键之前的条目；否则，返回 `None`。
    pub(crate) fn put(&mut self, key: Key, entry: MemIndexEntry) -> Option<MemIndexEntry> {
        self.map.insert(key, entry)
    }
    /// 从内存索引中删除与给定键关联的条目。
    ///
    /// # 参数
    /// - `key`: 要删除的条目在内存索引中的唯一键。
    ///
    /// # 返回值
    /// - `Option<MemIndexEntry>`: 如果成功删除了条目，则返回 Some(被删除的条目)；
    ///   如果没有找到与给定键关联的条目，则返回 None。
    pub(crate) fn delete(&mut self, key: &Key) -> Option<MemIndexEntry> {
        self.map.remove(key)
    }
    /// 获取集合的当前大小。
    ///
    /// 此方法返回集合中当前元素的数量。它通过检查内部映射的长度来实现这一点，
    /// 因为集合的大小直接对应于映射的键值对数量。
    ///
    /// # 返回
    ///
    /// * `usize` - 集合中元素的数量。
    pub(crate) fn size(&self) -> usize {
        self.map.len()
    }
}

/// `MemIndexIterator` 是一个用于迭代内存索引项的结构体。
///
/// 它持有另一个迭代器 `IntoIter`，用于遍历内存索引中的键值对。
/// 该结构体的主要用途是在内存中直接迭代索引项，而不是操作具体的存储数据。
/// 这在实现数据库、缓存或其他需要高效内存访问的数据结构时非常有用。
pub(crate) struct MemIndexIterator {
    inner: IntoIter<Key, MemIndexEntry>,
}

impl IntoIterator for MemIndexStorage {
    type Item = (Key, MemIndexEntry);
    type IntoIter = MemIndexIterator;

    /// 将当前结构体转换为其内部迭代器
    ///
    /// # 返回值
    /// 返回一个`MemIndexIterator`类型的迭代器，该迭代器允许访问结构体内部的元素
    ///
    /// # 示例
    ///
    fn into_iter(self) -> Self::IntoIter {
        // 将内部map结构转换为迭代器
        let inner = self.map.into_iter();
        // 构造并返回一个MemIndexIterator实例，该实例包裹了内部map的迭代器
        MemIndexIterator { inner }
    }
}

impl Iterator for MemIndexIterator {
    type Item = (Key, MemIndexEntry);

    /// 获取迭代器中的下一个元素
    ///
    /// # 返回值
    /// - `Some(T)`：如果迭代器中仍有元素，返回下一个元素
    /// - `None`：如果迭代器已经没有更多元素可迭代
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}


