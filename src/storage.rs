use crate::bitcask::{Key, PutOption, Value};
use crate::disk_logs::DiskLogFileStorage;
use crate::error::BitCaskError;
use crate::log_entry::DiskLogEntry;
use crate::log_file::DiskLogFile;
use crate::memory_index::MemIndexStorage;
use std::path::PathBuf;
use tracing::error;

/// `LogStorage` 结构体用于管理日志的存储。
/// 它主要负责在磁盘上存储日志数据，并在内存中维护索引，以便快速检索。
pub struct LogStorage {
    /// 存储日志数据的目录路径。
    data_dir: PathBuf,

    /// 用于在磁盘上持久化日志的 `DiskLog` 实例。
    disk_log: DiskLogFileStorage,

    /// 用于在内存中快速查找日志条目的 `MemIndex` 实例。
    mem_index: MemIndexStorage,
}
impl LogStorage {
    /// 创建一个新的BitCask实例。
    ///
    /// # 参数
    /// - `data_dir`: 数据目录的路径，可以是任何可以转换为`PathBuf`的类型。
    ///
    /// # 返回
    /// 返回一个`Result`，在成功创建BitCask实例时包含`Ok(Self)`，
    /// 在遇到错误时包含`Err(BitCaskError)`。
    pub fn new<T: Into<PathBuf>>(data_dir: T) -> Result<Self, BitCaskError> {
        
        // 将输入的数据目录路径转换为`PathBuf`类型
        let data_dir: PathBuf = data_dir.into();
        
        // 确保数据目录已经存在，如果不存在则创建它
        std::fs::create_dir_all(&data_dir)?;
        
        // 创建一个新的内存索引实例
        let mut mem_index = MemIndexStorage::new();
        
        // 从磁盘上的数据目录和内存索引中恢复磁盘日志
        let disk_log = DiskLogFileStorage::from_disk(&data_dir, &mut mem_index)?;
        
        // 成功创建BitCask实例后返回`Ok`
        Ok(Self {
            data_dir,
            disk_log,
            mem_index,
        })
    }

    /// 准备数据压缩
    ///
    /// 此函数负责准备数据压缩的过程它首先创建一个新的空日志文件，然后返回所有不可变文件和内存索引
    /// 这是数据压缩过程中的关键步骤，旨在优化数据库性能和存储空间使用效率
    ///
    /// 返回:
    ///     结果中包含一个可变长度的路径列表，这些路径指向所有不可变的文件如果操作成功，这些文件将被用于后续的压缩过程
    ///     如果操作失败，则返回相应的错误
    pub(crate) fn prepare_compaction(&mut self) -> Result<Vec<PathBuf>, BitCaskError> {
        // step 0: create a new empty log file
        self.disk_log.create_new_file()?;
        // step 1: return the immutable files and the mem_index
        let immutable_files = self.disk_log.get_immutable_files();
        Ok(immutable_files)
    }

    /// 完成压缩过程
    ///
    /// 此函数负责完成压缩的最后几个步骤：
    /// 1. 除不可变文件外，将其他文件复制到新目录
    /// 2. 根据新的日志文件初始化一个新的 DiskLog 和 MemIndex
    /// 3. 更新数据目录为新的日志文件路径
    ///
    /// 参数:
    /// - immutable_files: 不可变文件的路径列表，这些文件不会被复制
    /// - new_log_files_dir: 新日志文件的路径
    ///
    /// 返回:
    /// - 结果类型 `Result<(), BitCaskError>` 表示操作的成功或失败以及可能的错误信息
    pub(crate) fn finish_compaction(
        &mut self,
        immutable_files: Vec<PathBuf>,
        new_log_files_dir: PathBuf,
    ) -> Result<(), BitCaskError> {
        // step 3: copy the files to the new directory except the immutable files
        self.disk_log.copy_files_to_new_dir(immutable_files, new_log_files_dir.clone())?;
        // step 4: initialize a new DiskLog and MemIndex from the new log file
        let mut mem_index = MemIndexStorage::new();
        let disk_log = DiskLogFileStorage::from_disk(&new_log_files_dir, &mut mem_index)?;
        self.disk_log = disk_log;
        self.mem_index = mem_index;
        self.data_dir = new_log_files_dir.into();
        Ok(())
    }

    /// 根据键获取值，此函数仅在crate内部公开
    ///
    /// # 参数
    /// - `key`: 需要查询的键，类型为`&Key`
    ///
    /// # 返回值
    /// - 返回`Option<Value>`类型，如果找到值则为`Some(Value)`，否则为`None`
    ///
    /// 此函数首先在内存索引中查找键，如果找到且键未被标记为删除（墓碑），则从磁盘日志中获取对应值
    /// 如果在获取值的过程中发生错误，将打印错误信息并返回`None`
    pub(crate) fn get(&self, key: &Key) -> Option<Value> {
        // 在内存索引中查找键
        let mem_index_entry = self.mem_index.get(key);
        match mem_index_entry {
            // 如果找到键的条目
            Some(mem_index_entry) => {
                // 如果条目被标记为删除（墓碑），则返回None
                if mem_index_entry.is_tombstone() {
                    return None;
                }
                // 从磁盘日志中获取对应值
                let res = self.disk_log.get(&mem_index_entry);
                match res {
                    // 如果成功获取到值
                    Ok(value) => Some(value),
                    // 如果发生错误，打印错误信息并返回None
                    Err(e) => {
                        error!("Error while getting value from disk log: {:?}", e);
                        None
                    }
                }
            }
            // 如果在内存索引中未找到键，则返回None
            None => None,
        }
    }

    /// 向BitCask数据结构中插入或更新键值对。
    ///
    /// 此函数根据提供的选项（`option`）来决定插入行为。如果选项指定为`nx`，则当键不存在时进行插入；
    /// 如果选项指定为`xx`，则当键已存在时进行更新。如果没有指定选项，则执行默认的插入或更新操作。
    ///
    /// # 参数
    /// - `key`: 要插入或更新的键的引用。
    /// - `value`: 要插入或更新的值的引用。
    /// - `option`: 可能包含插入选项的`Some`或`None`。
    ///
    /// # 返回
    /// - `Result<(), BitCaskError>`: 表示操作结果，如果操作成功则返回`Ok(())`，否则返回包含错误的`Err`。
    pub(crate) fn put(
        &mut self,
        key: &Key,
        value: &Value,
        option: Option<PutOption>,
    ) -> Result<(), BitCaskError> {
        match option {
            Some(option) => {
                if option.nx {
                    // 当`nx`选项为真，且键不存在时进行插入。
                    return self.put_nx(key, value);
                }
                if option.xx {
                    // 当`xx`选项为真，且键已存在时进行更新。
                    return self.put_xx(key, value);
                }
                // 当`nx`和`xx`选项都为假，执行不含选项的插入或更新。
                self.put_without_option(key, value)
            }
            None => {
                // 当没有提供任何选项时，执行不含选项的插入或更新。
                self.put_without_option(key, value)
            }
        }
    }

    /// 将键值对写入磁盘日志中，并在内存索引中记录其位置
    ///
    /// 此函数直接将数据写入磁盘日志，而不涉及选项的选择或处理它主要用于在确定不需要处理
    /// 选项的情况下高效地存储数据
    ///
    /// # 参数
    /// - `key`: 键，用于标识要存储的值
    /// - `value`: 要存储的值
    ///
    /// # 返回值
    /// - `Result<(), BitCaskError>`: 表示操作是否成功的结果类型如果操作成功，返回`Ok(())`；
    ///   否则，返回包含错误信息的`Err`
    ///
    /// # 错误
    /// - `BitCaskError`: 可能的错误包括但不限于磁盘写入错误、键值问题等
    pub(crate) fn put_without_option(
        &mut self,
        key: &Key,
        value: &Value,
    ) -> Result<(), BitCaskError> {
        // 将键值对写入磁盘日志，获取对应的索引条目
        let index_entry = self.disk_log.put(key, value)?;
        // 将键和对应的索引条目存入内存索引中，以便后续快速查找
        self.mem_index.put(key.clone(), index_entry);
        // 返回操作成功的结果
        Ok(())
    }

    /// 在BitCask中插入键值对，如果键已存在且不是墓碑，则返回错误
    ///
    /// # 参数
    /// - `key`: 键，用于标识值
    /// - `value`: 待插入的值
    ///
    /// # 返回
    /// - `Result<(), BitCaskError>`: 如果插入成功，则返回`Ok(())`；如果键已存在且不是墓碑，则返回`Err(BitCaskError::KeyExists)`；其他错误情况返回相应的`BitCaskError`
    ///
    /// # 说明
    /// 此方法用于向BitCask存储中插入一个键值对。首先检查内存索引中是否已存在该键，如果存在且不是墓碑，则拒绝插入。如果键不存在或是一个墓碑，则将键值对写入磁盘日志，并更新内存索引。
    fn put_nx(&mut self, key: &Key, value: &Value) -> Result<(), BitCaskError> {
        
        // 从内存索引中获取键对应的条目
        let index_entry = self.mem_index.get(key);
        
        // 检查键是否已存在且不是墓碑
        if let Some(index_entry) = index_entry {
            if !index_entry.is_tombstone() {
                return Err(BitCaskError::KeyExists);
            }
        }
        
        // 将键值对写入磁盘日志，并获取写入的条目
        let index_entry = self.disk_log.put(key, value)?;
        
        // 更新内存索引
        self.mem_index.put(key.clone(), index_entry);
        
        Ok(())
    }

    /// 更新给定键的值，如果键已存在且不是墓碑，则更新磁盘日志和内存索引。
    ///
    /// # 参数
    /// - `key`: 需要更新的键引用。
    /// - `value`: 需要存储的新值引用。
    ///
    /// # 返回
    /// - `Result<(), BitCaskError>`: 如果操作成功，则返回 `Ok(())`；否则返回错误类型 `BitCaskError`。
    ///
    /// # 错误
    /// - `BitCaskError::KeyNotFound`: 当键不存在或键是墓碑时触发。
    pub(crate) fn put_xx(&mut self, key: &Key, value: &Value) -> Result<(), BitCaskError> {
       
        // 检查内存索引中是否已存在给定键
        let index_entry = self.mem_index.get(key);
       
        // 如果找到索引项且不是墓碑，则继续操作
        if let Some(index_entry) = index_entry {
            if index_entry.is_tombstone() {
                return Err(BitCaskError::KeyNotFound);
            }
        } else {
            return Err(BitCaskError::KeyNotFound);
        }
        
        // 在磁盘日志中更新键的值，并获取新的索引项
        let index_entry = self.disk_log.put(key, value)?;
        
        // 将新的索引项更新到内存索引中
        self.mem_index.put(key.clone(), index_entry);
        
        Ok(())
    }

    /// 从BitCask存储中删除指定键的数据。
    ///
    /// # 参数
    /// - `key`: 要删除的数据的键。
    ///
    /// # 返回
    /// - `Result<(), BitCaskError>`: 如果删除成功，则返回`Ok(())`，否则返回`Err`包含错误信息。
    ///
    /// # 描述
    /// 此函数负责删除给定键对应的数据。首先，它会调用磁盘日志的删除方法来实际删除数据，
    /// 然后将该删除操作的索引条目更新到内存索引中，以保持数据的一致性。
    pub(crate) fn delete(&mut self, key: &Key) -> Result<(), BitCaskError> {
        let index_entry = self.disk_log.delete(key)?;
        self.mem_index.put(key.clone(), index_entry);
        Ok(())
    }

    /// 获取当前对象的大小
    ///·
    /// 返回值为当前对象占用的内存大小，以字节为单位
    pub(crate) fn size(&self) -> usize {
        self.mem_index.size()
    }
}

/// 开始压缩
///
/// 此函数负责将一组不可变文件中的数据合并到一个新的日志文件中。
/// 它首先创建一个新的日志文件，然后遍历内存索引中的条目，并将它们
/// 的值写入新的日志文件中。这是数据压缩和整理过程的一部分，旨在
/// 回收磁盘空间和提高数据库的查询效率。
///
/// 参数:
/// - immutable_files: 一个包含不可变文件路径的向量。
/// - new_log_file_path: 新日志文件的路径。
///
/// 返回:
/// - 结果类型 `Result<(), BitCaskError>` 表示操作的成功或失败以及可能的错误信息。
pub(crate) fn start_compaction(
    immutable_files: Vec<PathBuf>,
    new_log_file_path: PathBuf,
) -> Result<(), BitCaskError> {
    // 创建新的日志文件的目录
    std::fs::create_dir_all(&new_log_file_path)?;
    // 初始化新的日志文件对象
    let mut new_log_file = DiskLogFile::new(&new_log_file_path, 0)?;
    // 初始化内存索引对象
    let mut mem_index = MemIndexStorage::new();
    // 使用不可变文件初始化磁盘日志对象
    let disk_logs = DiskLogFileStorage::immutable_initialization(immutable_files, &mut mem_index)?;
    // 创建内存索引的迭代器
    let iter = mem_index.into_iter();
    // 遍历内存索引中的每个条目
    for (key, mem_index_entry) in iter {
        // 根据内存索引条目从磁盘日志中获取对应的值
        let value = disk_logs.get(&mem_index_entry)?;
        // 创建一个新的磁盘日志条目
        let disk_log_entry = DiskLogEntry::new_entry(key, value);
        // 将新的磁盘日志条目写入新的日志文件中
        new_log_file.append_new_entry(disk_log_entry)?;
    }
    // 返回Ok(())表示操作成功
    Ok(())
}
