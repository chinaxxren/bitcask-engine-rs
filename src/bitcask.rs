use crate::error::BitCaskError;
use crate::storage::{start_compaction, LogStorage};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub(crate) type FileId = usize;
pub(crate) type ByteSize = u64;
pub(crate) type ByteOffset = u64;
pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

/// 定义一个键值对存储的公共 trait，用于在键值存储系统中规范数据的读取、写入和删除操作。
/// 实现该 trait 的类型还需要实现 Clone、Send，并且其生命周期为 'static，以确保数据可以在多线程环境中安全地发送和持久存储。
pub trait KVStorage: Clone + Send + 'static {
    /// 根据给定的键获取对应的值。
    /// # 参数
    /// - `key`: 一个指向 Key 类型的引用，表示要查找的键。
    /// # 返回值
    /// - `Option<Value>`: 如果找到了键对应的值，则返回 Some 包裹的值；否则返回 None。
    fn get(&self, key: &Key) -> Option<Value>;

    /// 使用给定的值和选项参数将一个键值对存入存储系统。
    /// # 参数
    /// - `key`: 一个指向 Key 类型的引用，表示要存储的键。
    /// - `value`: 一个指向 Value 类型的引用，表示要存储的值。
    /// - `option`: 一个 Option 类型的 PutOption，用于控制存储操作的选项。
    /// # 返回值
    /// - `Result<(), BitCaskError>`: 如果存储成功，则返回 Ok(()); 否则返回 Err 包裹的错误。
    fn put_with_option(&mut self, key: &Key, value: &Value, option: Option<PutOption>) -> Result<(), BitCaskError>;

    /// 将一个键值对存入存储系统，使用默认的存储选项。
    /// 该函数是 `put_with_option` 函数的一个简化版本，使用 PutOption::none() 作为存储选项。
    /// # 参数
    /// - `key`: 一个指向 Key 类型的引用，表示要存储的键。
    /// - `value`: 一个指向 Value 类型的引用，表示要存储的值。
    /// # 返回值
    /// - `Result<(), BitCaskError>`: 如果存储成功，则返回 Ok(()); 否则返回 Err 包裹的错误。
    fn put(&mut self, key: &Key, value: &Value) -> Result<(), BitCaskError> {
        self.put_with_option(key, value, PutOption::none())
    }

    /// 删除存储系统中与给定键关联的值。
    /// # 参数
    /// - `key`: 一个指向 Key 类型的引用，表示要删除的键。
    /// # 返回值
    /// - `Result<(), BitCaskError>`: 如果删除成功，则返回 Ok(()); 否则返回 Err 包裹的错误。
    fn delete(&mut self, key: &Key) -> Result<(), BitCaskError>;

    /// 获取存储系统中当前存储的键值对数量。
    /// # 返回值
    /// - `usize`: 表示存储系统中键值对的数量。
    fn size(&self) -> usize;
}

/// 定义一个名为PutOption的公开结构体，用于封装存储操作的选项。
/// 结构体包含两个布尔类型字段：nx和xx，分别表示操作的条件。
/// NX (not exist) for put operation
/// XX (exist) for put operation
pub struct PutOption {
    pub nx: bool,
    pub xx: bool,
}

impl PutOption {

    /// 返回一个Option类型的None值，表示没有PutOption实例。
    /// 这个方法用于在不需要实例时提供一个明确的None返回值。
    pub fn none() -> Option<Self> {
        None
    }

    /// 创建一个PutOption的实例，并设置nx字段为true，xx字段为false。
    /// 这个方法用于明确需要使用nx条件的操作选项。
    pub fn nx() -> Option<Self> {
        Some(Self {
            nx: true,
            xx: false,
        })
    }

    /// 创建一个PutOption的实例，并设置xx字段为true，nx字段为false。
    /// 这个方法用于明确需要使用xx条件的操作选项。
    pub fn xx() -> Option<Self> {
        Some(Self {
            nx: false,
            xx: true,
        })
    }
}

#[derive(Clone)]
// 定义一个BitCask结构体，用于管理存储引擎
pub struct BitCask {
    pub(crate) storage: Arc<RwLock<LogStorage>>,
}

impl BitCask {
    // 创建一个新的BitCask实例
    // 参数: data_dir - 存储数据的目录路径
    // 返回: Result<Self, BitCaskError> - 如果成功创建实例则返回Ok，否则返回Err
    pub fn new<T: Into<PathBuf>>(data_dir: T) -> Result<Self, BitCaskError> {
        let storage = LogStorage::new(data_dir)?;
        Ok(Self {
            storage: Arc::new(RwLock::new(storage)),
        })
    }

    // 注意：此方法是一个阻塞调用，它将阻塞当前线程直到合并完成
    // 如果在异步上下文中使用此方法，你应该在一个阻塞工作线程中调用它
    // 参数: data_dir - 新的存储数据的目录路径
    // 返回: Result<(), BitCaskError> - 如果合并成功则返回Ok(()), 否则返回Err
    pub fn compact_to_new_dir<T: Into<PathBuf>>(&self, data_dir: T) -> Result<(), BitCaskError> {
        let mut storage = self.storage.write().unwrap();
        let data_dir: PathBuf = data_dir.into();
        let immutable_files = storage.prepare_compaction()?;
        drop(storage);
        start_compaction(immutable_files.clone(), data_dir.clone())?;
        let mut storage = self.storage.write().unwrap();
        storage.finish_compaction(immutable_files, data_dir)
    }
}

// 实现KVStorage trait
impl KVStorage for BitCask {
    // 根据给定的键获取值
    // 参数: key - 要查找的键
    // 返回: Option<Value> - 如果键存在则返回Some(value)，否则返回None
    fn get(&self, key: &Key) -> Option<Value> {
        self.storage.read().unwrap().get(key)
    }

    // 带选项地将键值对放入存储中
    // 参数: key - 要放入的键
    //        value - 要放入的值
    //        option - 放入选项
    // 返回: Result<(), BitCaskError> - 如果放入成功则返回Ok(()), 否则返回Err
    fn put_with_option(&mut self, key: &Key, value: &Value, option: Option<PutOption>) -> Result<(), BitCaskError> {
        self.storage.write().unwrap().put(key, value, option)
    }

    // 删除给定的键
    // 参数: key - 要删除的键
    // 返回: Result<(), BitCaskError> - 如果删除成功则返回Ok(()), 否则返回Err
    fn delete(&mut self, key: &Key) -> Result<(), BitCaskError> {
        self.storage.write().unwrap().delete(key)
    }

    // 获取存储的大小
    // 返回: usize - 存储的大小
    fn size(&self) -> usize {
        self.storage.read().unwrap().size()
    }
}