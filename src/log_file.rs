use crate::bitcask::FileId;
use crate::error::BitCaskError;
use crate::log_entry::{Deserialize, DiskLogEntry, Serialize};
use crate::memory_index::{MemIndexStorage, MemIndexEntry};
use std::io::{BufReader, Seek, SeekFrom, Write};
use std::path::PathBuf;
use tracing::trace;

/// `DiskLogFile` 结构体代表一个磁盘上的日志文件。
/// 它包含了文件的唯一标识符、文件路径和文件对象。
///
/// # Fields
/// - `file_id`: 文件的唯一标识符，用于在文件之间进行区分。
/// - `path`: 文件在磁盘上的路径，用于定位文件。
/// - `file`: 文件的句柄，用于对文件进行读写操作。
pub(crate) struct DiskLogFile { // DataFile
    pub(crate) file_id: FileId,
    pub(crate) path: PathBuf,
    pub(crate) file: std::fs::File,
}

impl DiskLogFile {
    pub(crate) const EXT: &'static str = "bitcask";
    pub(crate) const MAX_FILE_SIZE: u64 = 1024 * 1024 * 1024; // 1GB

    /// 创建一个新的文件用于写入
    ///
    /// # 参数
    /// - `data_dir`: 数据目录的路径，可以转换为 `PathBuf`
    /// - `file_id`: 文件的唯一标识符，类型为 `FileId`
    ///
    /// # 返回
    /// - `Result<Self, BitCaskError>`: 返回一个结果，其中 Ok 包含一个文件对象 `Self`，Err 包含一个错误对象 `BitCaskError`
    ///
    /// # 说明
    /// 该函数根据给定的数据目录和文件 ID 构建文件路径，并创建一个新的文件用于写入
    pub(crate) fn new<T: Into<PathBuf>>(
        data_dir: T,
        file_id: FileId,
    ) -> Result<Self, BitCaskError> {
        
        // 将数据目录转换为 PathBuf 对象
        let mut path: PathBuf = data_dir.into();
        
        // 将文件 ID 添加到路径中
        path.push(file_id.to_string());
        
        // 设置文件扩展名
        path.set_extension(Self::EXT);
        
        // 使用 OpenOptions 创建、读取和追加模式打开文件
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        
        // 返回 Ok 包含一个文件对象，其中包含文件 ID、路径和文件描述符
        Ok(Self {
            file_id,
            path,
            file,
        })
    }

    // 打开一个现有文件以进行读取
    pub(crate) fn open(
        file_id: FileId,
        path: PathBuf,
        mem_index: &mut MemIndexStorage,
    ) -> Result<Self, BitCaskError> {
        
        // 这里所有的文件都以追加模式打开，但除了最后一个文件外，我们实际上并不追加任何内容
        trace!("opening disk log file: {:?}", path);
        
        // 创建文件的打开选项，并设置读取和追加权限
        let file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path)?;
        
        // 使用给定的文件ID、路径和文件对象来创建一个新的FileLog实例
        let file = Self {
            file_id,
            path,
            file,
        };
        
        // 用内存索引填充文件，以便于快速查找文件中的数据
        file.populate_mem_index(mem_index)?;
        
        // 返回成功的结果
        Ok(file)
    }

    /// 从磁盘日志文件中加载数据到内存索引中。
    ///
    /// 该函数的目的是将持久化在磁盘日志文件中的所有有效条目加载到内存索引结构中，
    /// 以加速后续的检索操作。它会忽略那些标记为墓碑（表示删除）的条目。
    ///
    /// # 参数
    /// - `mem_index`: 一个可变引用，指向内存索引结构，该结构用于存储条目的键和其在磁盘文件中的位置信息。
    ///
    /// # 返回
    /// - `Result<(), BitCaskError>`: 表示操作结果，如果成功则返回 `Ok(())`，否则返回包含错误信息的 `Err`。
    ///
    /// # 错误
    /// - 如果文件元数据获取失败，或者文件读取操作中发生错误，将返回 `BitCaskError`。
    fn populate_mem_index(&self, mem_index: &mut MemIndexStorage) -> Result<(), BitCaskError> {
       
        // 获取文件的大小，用于确定读取的终点。
        let file_size = self.file.metadata()?.len();
        
        // 创建一个缓冲读取器，用于高效读取文件内容。
        let mut buffered_reader = BufReader::new(&self.file);
       
        // 初始化读取位置指针。
        let mut cursor = 0u64;
        
        // 将文件读取位置设置到开始位置。
        buffered_reader.seek(SeekFrom::Start(cursor))?;

        // 循环读取文件中的条目，直到文件末尾。
        loop {
            
            // 如果读取位置超过文件大小，则停止读取。
            if cursor >= file_size {
                break;
            }
            
            // 读取并反序列化一个条目。
            let entry: DiskLogEntry = DiskLogEntry::deserialize(&mut buffered_reader)?;
            
            // 计算条目总大小，用于更新读取位置。
            let entry_size = entry.total_byte_size();
            
            // 如果条目是墓碑（表示删除操作），则不在内存索引中存储。
            if entry.is_tombstone() {
                mem_index.delete(&entry.key);
            } else {
                // 创建一个内存索引条目，包含文件ID，值的偏移量和大小。
                let mem_log_entry = MemIndexEntry {
                    file_id: self.file_id,
                    value_offset: cursor + entry.value_byte_offset(),
                    value_size: entry.value_byte_size(),
                };
                // 将条目添加到内存索引中。
                mem_index.put(entry.key, mem_log_entry);
            }
            // 更新读取位置，指向下一个条目开始处。
            cursor += entry_size;
        }
        // 所有操作完成，返回Ok(())。
        Ok(())
    }

    /// 向日志文件中追加新的日志条目
    ///
    /// # 参数
    /// - `entry`: 待写入的日志条目
    ///
    /// # 返回值
    /// - `Ok(u64)`: 返回日志条目在文件中的偏移量
    /// - `Err(BitCaskError)`: 返回错误，如果写入过程中发生问题
    ///
    /// # 说明
    /// 此函数负责将一个新的日志条目追加到日志文件的末尾，并确保更改持久化到磁盘。
    /// 它首先计算出日志条目在文件中的位置（偏移量），然后将日志条目序列化到文件中，
    /// 最后刷新文件缓冲区以确保更改持久化。这个过程保证了日志条目的原子写入和持久化。
    pub(crate) fn append_new_entry(&mut self, entry: DiskLogEntry) -> Result<u64, BitCaskError> {
        let file = &mut self.file;
        let value_offset = file.seek(SeekFrom::End(0))? + entry.value_byte_offset();
        entry.serialize(file)?;
        file.flush()?; // 确保持久性
        Ok(value_offset)
    }
}
