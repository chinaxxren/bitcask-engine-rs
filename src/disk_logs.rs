use crate::bitcask::{FileId, Key, Value};
use crate::error::BitCaskError;
use crate::log_entry::DiskLogEntry;
use crate::log_file::DiskLogFile;
use crate::memory_index::{MemIndexEntry, MemIndexStorage};
use std::ffi::OsStr;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;
use tracing::trace;

/// `DiskLogFileStorage` 结构体用于管理磁盘日志。
/// 它主要负责维护一组日志文件（DiskLogFile）以及与日志文件相关的元数据。
pub(crate) struct DiskLogFileStorage {
    /// 日志文件的集合，每个日志文件可能包含多个日志条目。
    files: Vec<DiskLogFile>,

    /// 日志文件所在的目录路径。
    data_dir: PathBuf,

    /// 当前日志文件的大小，用于跟踪何时需要切换到新的日志文件。
    current_file_size: u64,

    /// 标识日志是否为不可变状态。一旦日志被标记为不可变，不能再向其写入日志条目。
    immutable: bool,
}

impl DiskLogFileStorage {
    /// 从不可变文件初始化磁盘日志。当开始压缩操作时调用此方法。
    ///
    /// # 参数
    /// - `immutable_files`: 一个包含不可变文件路径的向量。
    /// - `mem_index`: 一个指向内存索引的可变引用，用于更新内存中的索引信息。
    ///
    /// # 返回
    /// 返回一个结果，其中包含一个初始化后的`Self`实例（成功）或者一个`BitCaskError`（失败）。
    ///
    /// # 说明
    /// 此方法首先将不可变文件转换为磁盘日志文件格式，然后获取这些文件的父目录作为数据目录。
    /// 最后，使用这些文件、数据目录以及一些初始化标志（如当前文件大小为0和设置不可变为true）来创建并返回一个`Self`实例。
    pub(crate) fn immutable_initialization(
        immutable_files: Vec<PathBuf>,
        mem_index: &mut MemIndexStorage,
    ) -> Result<Self, BitCaskError> {
        // 将不可变文件转换为磁盘日志文件格式，并更新内存索引
        let files = Self::to_disk_log_files(immutable_files, mem_index)?;

        // 获取数据目录路径
        let data_dir = files.first().unwrap().path.parent().unwrap().to_path_buf();

        // 使用转换后的文件、数据目录路径以及其他初始化标志创建并返回一个新的实例
        Ok(Self {
            files,
            data_dir,
            current_file_size: 0,
            immutable: true,
        })
    }

    /// 创建一个新的日志文件，文件ID为0。
    ///
    /// # 参数
    /// - `data_dir`: 数据目录的路径，可以转换为`PathBuf`。
    ///
    /// # 返回值
    /// 返回`Result`类型，包含`Self`（当前实例）或者`BitCaskError`（如果创建过程中发生错误）。
    ///
    /// # 说明
    /// 此函数用于初始化一个新的日志文件管理器，它将在指定的数据目录中创建一个文件ID为0的日志文件。
    /// 这个管理器用来处理日志文件的创建、追踪当前文件的大小，并确保文件的不可变性。
    fn new<T: Into<PathBuf> + Clone>(data_dir: T) -> Result<Self, BitCaskError> {
        // 将数据目录路径转换为PathBuf类型，以便于文件操作。
        let data_dir_path_buf: PathBuf = data_dir.clone().into();
        // 创建一个新的日志文件管理器实例，包含一个文件ID为0的日志文件。
        Ok(Self {
            files: vec![DiskLogFile::new(data_dir, 0)?],
            data_dir: data_dir_path_buf,
            current_file_size: 0,
            immutable: false,
        })
    }

    /// If the data directory is empty, create a new log file with file id 0.
    /// 从磁盘加载所有日志文件并填充内存索引。
    ///
    /// # 参数
    /// - `data_dir`: 数据目录的路径，用于查找所有日志文件。
    /// - `mem_index`: 内存索引的引用，用于存储日志文件的内容。
    ///
    /// # 返回
    /// 返回结果类型为`Result<Self, BitCarkError>`，表示可能出错的初始化结果。
    ///
    /// # 错误
    /// 如果数据目录不存在或无法读取，或者当前文件大小无法获取，则返回`BitCarkError`。
    pub(crate) fn from_disk<T: Into<PathBuf>>(
        data_dir: T,
        mem_index: &mut MemIndexStorage,
    ) -> Result<Self, BitCaskError> {
        let data_dir: PathBuf = data_dir.into();

        // 读取数据目录下的所有文件，过滤出日志文件，并转换为`DiskLogFile`对象。
        let files = std::fs::read_dir(&data_dir)?
            .filter_map(|path| {
                path.ok().map(|path| path.path()).filter(|path| {
                    path.is_file() && path.extension() == Some(OsStr::new(DiskLogFile::EXT))
                })
            })
            .collect();
        let files = Self::to_disk_log_files(files, mem_index)?;

        // 如果没有找到日志文件，则从头开始创建新的实例。
        if files.is_empty() {
            trace!("No disk log files found, starting from scratch");
            return Self::new(data_dir);
        }

        // 获取最后一个日志文件的大小，作为当前文件大小。
        let current_file_size = files.last().unwrap().file.metadata()?.len();

        // 创建实例并返回。
        Ok(Self {
            files,
            data_dir,
            current_file_size,
            immutable: false,
        })
    }

    /**
     * 获取当前文件和文件ID
     *
     * 此方法用于返回当前正在使用的日志文件和其ID
     * 它首先找到已经打开的最后一个文件，然后返回该文件和它的ID
     *
     * # 返回值
     * - `&mut DiskLogFile`: 当前正在使用的日志文件，为可变借用
     * - `FileId`: 当前文件的ID
     */
    fn current_file(&mut self) -> (&mut DiskLogFile, FileId) {
        // the last file is always open for appending
        let disk_log_file = self.files.last_mut().unwrap();
        let file_id = disk_log_file.file_id;
        (disk_log_file, file_id)
    }

    /// 根据文件ID获取磁盘日志文件的引用
    ///
    /// # 参数
    /// - `file_id`: 文件ID，类型为`FileId`，用于标识特定的文件
    ///
    /// # 返回值
    /// 返回一个指向`DiskLogFile`类型的引用，该引用指向由`file_id`指定的文件
    ///
    /// # 说明
    /// 本方法通过将`file_id`转换为`usize`类型，从`self.files`集合中查找并获取对应的文件引用
    /// 使用`unwrap`方法处理查找结果，这意味着如果文件ID无效或文件不存在于集合中，程序将panic
    fn get_file(&self, file_id: FileId) -> &DiskLogFile {
        self.files.get(file_id as usize).unwrap()
    }

    /// 根据内存索引项获取磁盘中的值
    ///
    /// # 参数
    /// - `mem_index_entry`: 内存索引项的引用，包含值的偏移量、大小和文件ID
    ///
    /// # 返回
    /// - `Result<Value, BitCaskError>`: 返回一个结果，包含请求的值或操作中遇到的错误
    pub(crate) fn get(&self, mem_index_entry: &MemIndexEntry) -> Result<Value, BitCaskError> {
        // 解构内存索引项以获取值的偏移量、大小和文件ID
        let MemIndexEntry {
            value_offset,
            value_size,
            file_id,
        } = mem_index_entry;

        // 根据文件ID获取对应的磁盘日志文件
        let disk_log_file = self.get_file(*file_id);

        // 创建一个具有指定容量的缓冲读取器，以提高读取性能
        let mut buffered_reader =
            BufReader::with_capacity(*value_size as usize, &disk_log_file.file);

        // 将读取器定位到值的开始偏移量位置
        buffered_reader.seek(SeekFrom::Start(*value_offset))?;

        // 创建一个具有值大小的缓冲区，用于读取值
        let mut buf = vec![0u8; *value_size as usize];

        // 从缓冲读取器中精确读取值到缓冲区
        buffered_reader.read_exact(buf.as_mut())?;

        // 将读取到的缓冲区转换为值对象并返回
        Ok(Value::from(buf))
    }

    /// 向内存索引中插入键值对
    ///
    /// # 参数
    /// - `key`: 键的引用
    /// - `value`: 值的引用
    ///
    /// # 返回
    /// 返回结果类型`Result`，在成功插入后包含`MemIndexEntry`类型的条目信息，否则包含`BitCaskError`类型的错误信息
    ///
    /// # 说明
    /// 此函数通过克隆键和值，并创建一个新的`DiskLogEntry`条目，将其追加到内存索引中
    pub(crate) fn put(&mut self, key: &Key, value: &Value) -> Result<MemIndexEntry, BitCaskError> {
        self.append_log_entry(DiskLogEntry::new_entry(key.clone(), value.clone()))
    }

    /// 从内存索引中删除指定键对应的条目
    ///
    /// # 参数
    /// - `key`: 需要删除的条目在内存索引中的键
    ///
    /// # 返回
    /// - `Result<MemIndexEntry, BitCaskError>`: 如果删除成功，返回被删除的条目；否则返回错误
    ///
    /// # 说明
    /// 此函数通过向磁盘日志添加一个表示删除操作的条目来标记对应键的条目为已删除状态
    /// 它并不直接从内存索引中移除条目，而是通过添加一个删除标记（tombstone）来实现逻辑删除
    pub(crate) fn delete(&mut self, key: &Key) -> Result<MemIndexEntry, BitCaskError> {
        self.append_log_entry(DiskLogEntry::new_tombstone(key.clone()))
    }

    /// 向当前磁盘日志文件中追加新的日志条目。
    ///
    /// # 参数
    /// - `entry`: 待追加的日志条目。
    ///
    /// # 返回
    /// 成功时返回内存索引条目，包含文件ID、值偏移量和值大小；失败时返回`BitCaskError`。
    ///
    /// # 错误
    /// 如果当前磁盘日志文件是不可变的，则会触发恐慌。
    ///
    /// # 说明
    /// 此函数负责将新的日志条目追加到当前的磁盘日志文件中，并更新当前文件大小。
    /// 如果当前文件大小超过最大文件大小，将创建一个新的文件。
    fn append_log_entry(&mut self, entry: DiskLogEntry) -> Result<MemIndexEntry, BitCaskError> {
        // 检查当前磁盘日志文件是否为不可变状态，如果是，则触发恐慌。
        if self.immutable {
            panic!("Cannot append to an immutable disk log");
        }

        // 获取当前正在使用的磁盘日志文件和文件ID。
        let (disk_log_file, file_id) = self.current_file();

        // 将新的日志条目追加到磁盘日志文件中，并获取该条目的偏移量。
        let value_offset = disk_log_file.append_new_entry(entry.clone())?;

        // 更新当前文件大小。
        self.current_file_size += entry.total_byte_size();

        // 检查当前文件大小是否超过最大文件大小，如果超过，则创建一个新的文件。
        if self.current_file_size > DiskLogFile::MAX_FILE_SIZE {
            self.check_file_size()?;
        }

        // 返回内存索引条目，包含文件ID、值偏移量和值大小。
        Ok(MemIndexEntry {
            file_id,
            value_offset,
            value_size: entry.value_byte_size(),
        })
    }

    /// 检查当前日志文件的大小
    ///
    /// 此函数用于检查当前日志文件是否超过了最大文件大小限制。如果超过，则关闭当前文件并创建一个新的文件。
    /// 这是为了防止单个日志文件无限增长，导致性能问题或磁盘空间不足。
    ///
    /// # Returns
    /// * `Result<(), BitCaskError>` - 表示操作的成功或失败，如果操作失败，则返回相应的错误。
    fn check_file_size(&mut self) -> Result<(), BitCaskError> {
        // 获取当前正在使用的日志文件和文件ID
        let (disk_log_file, file_id) = self.current_file();
        // 通过文件ID获取文件对象
        let file = &mut disk_log_file.file;
        // 获取文件的元数据，包括文件大小等信息
        let file_size = file.metadata()?.len();
        // 检查文件大小是否超过了最大文件大小限制
        if file_size > DiskLogFile::MAX_FILE_SIZE {
            // 如果文件过大，记录日志并创建新文件
            trace!(
                "Disk log file {} exceeds max file size, creating a new file",
                file_id
            );
            self.create_new_file()?;
        }
        // 返回成功
        Ok(())
    }

    /// 获取不可变文件路径集合
    ///
    /// 此函数旨在筛选出所有非最新的日志文件，通过文件ID进行区分
    /// 它首先确定最新的文件ID，然后筛选出所有其他文件
    /// 这在需要对日志文件进行操作，同时保持最新文件不变的情况下非常有用
    ///
    /// # 返回值
    ///
    /// 返回一个`Vec<PathBuf>`类型，包含所有非最新文件的路径
    pub fn get_immutable_files(&self) -> Vec<PathBuf> {
        // 确定最新文件的文件ID
        let last_file_id = self.files.last().unwrap().file_id;

        // 筛选、复制除最新文件之外的所有文件的路径，并收集到向量中
        self.files
            .iter()
            .filter(|disk_log_file| disk_log_file.file_id != last_file_id)
            .map(|disk_log_file| disk_log_file.path.clone())
            .collect()
    }

    /// 当用户调用`compact_to_new_dir`或库函数`check_file_size`时被调用，负责创建一个新的日志文件。
    pub(crate) fn create_new_file(&mut self) -> Result<(), BitCaskError> {
        // 获取当前最后一个文件的ID，为新文件生成递增的ID。
        let last_file_id = self.files.last().unwrap().file_id;
        let new_file_id = last_file_id + 1;

        // 基于新的文件ID创建一个新的日志文件实例。
        let new_file = DiskLogFile::new(&self.data_dir, new_file_id)?;

        // 将新的日志文件实例添加到文件集合中。
        self.files.push(new_file);

        // 表示新文件创建成功，无错误返回。
        Ok(())
    }

    /// 将文件复制到新目录，同时排除不可变文件
    ///
    /// # 参数
    /// - `immutable_files`: 不可变文件的路径列表，这些文件不会被复制
    /// - `new_log_file_path`: 新日志文件的目录路径，不包含文件名
    ///
    /// # 返回
    /// - `Result<(), BitCaskError>`: 表示操作结果，如果操作成功则返回 `Ok(())`，否则返回错误 `BitCaskError`
    ///
    /// # 说明
    /// 此函数首先从 `self.files` 中过滤出不在 `immutable_files` 列表中的文件，然后将这些可变文件复制到新的日志文件目录中
    pub(crate) fn copy_files_to_new_dir(
        &self,
        immutable_files: Vec<PathBuf>,
        new_log_file_path: PathBuf,
    ) -> Result<(), BitCaskError> {
        // 从 self.files 中排除不可变文件，只保留需要复制的可变文件
        let mut files: Vec<PathBuf> = self
            .files
            .iter()
            .filter(|disk_log_file| !immutable_files.contains(&disk_log_file.path))
            .map(|disk_log_file| disk_log_file.path.clone())
            .collect();

        // 将筛选后的文件复制到新的目录中
        for file in files.iter_mut() {
            // 构建新的文件路径
            let mut new_file = new_log_file_path.clone();
            new_file.push(file.file_name().unwrap());
            // 执行文件复制操作
            std::fs::copy(file, new_file)?;
        }

        // 返回操作成功
        Ok(())
    }

    /// 将给定的文件集合转换为磁盘日志文件集合
    ///
    /// # 参数
    /// - `files`: 一个包含文件路径的向量
    /// - `mem_index`: 一个内存索引存储的引用，用于与磁盘日志文件交互
    ///
    /// # 返回
    /// 返回一个结果，包含一个磁盘日志文件的向量，或者一个`BitCaskError`错误
    ///
    /// # 错误
    /// 如果文件ID解析失败，或者磁盘日志文件打开失败，或者文件排序失败，则返回错误
    pub(crate) fn to_disk_log_files(
        files: Vec<PathBuf>,
        mem_index: &mut MemIndexStorage,
    ) -> Result<Vec<DiskLogFile>, BitCaskError> {
        // 过滤并映射文件路径，解析文件ID，并尝试打开每个文件作为磁盘日志文件
        let mut files = files
            .into_iter()
            .filter_map(|path| {
                path.file_stem()
                    .and_then(|file_stem| file_stem.to_str())
                    .and_then(|file_stem| file_stem.parse::<FileId>().ok())
                    .map(|file_id| (file_id, path))
            })
            .map(|(file_id, path)| {
                DiskLogFile::open(file_id, path, mem_index)
                    .map(|disk_log_file| (file_id, disk_log_file))
            })
            .collect::<Result<Vec<(FileId, DiskLogFile)>, BitCaskError>>()?;

        // 按文件ID排序磁盘日志文件
        files.sort_by_key(|(file_id, _)| *file_id);

        // 转换元组向量为磁盘日志文件向量并返回
        Ok(files
            .into_iter()
            .map(|(_, disk_log_file)| disk_log_file)
            .collect())
    }
}
