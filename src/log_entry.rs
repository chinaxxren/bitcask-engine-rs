use crate::bitcask::{ByteOffset, ByteSize, Key, Value};
use crate::error::BitCaskError;
use crc::{Crc, CRC_32_CKSUM};
use std::io::{Read, Write};

const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_CKSUM);

/// Any object that is readable can be deserialized
pub(crate) trait Deserialize {
    fn deserialize<T: Read>(buf: &mut T) -> Result<Self, BitCaskError>
    where
        Self: Sized;
}

/// Any object that is writable can be serialized to
pub(crate) trait Serialize {
    fn serialize<T: Write>(&self, buf: &mut T) -> Result<(), BitCaskError>;
}

/// DiskLogEntry is a memory representation of a key-value pair that is persisted in disk.
/// 表示磁盘日志条目的结构体。
///
/// `DiskLogEntry` 用于存储日志条目，其中包括校验和、键和可选的值。
/// 如果值为 None，则表示该条目为删除标记（tombstone）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiskLogEntry {
    /// 日志条目的校验和，用于校验数据的完整性。
    pub(crate) check_sum: u32,
    /// 日志条目的键，唯一标识一个数据项。
    pub(crate) key: Key,
    /// 日志条目的值，如果为 None，则表示该条目为删除标记。
    pub(crate) value: Option<Value>, // None 表示一个删除标记
}

impl DiskLogEntry {
    /// 创建一个新的条目。
    ///
    /// # 参数
    /// - `key`: 条目的键。
    /// - `value`: 条目的值。
    ///
    /// # 返回值
    /// 返回一个包含给定键和值的条目实例。
    ///
    /// # 说明
    /// 此函数用于初始化一个新的条目对象，计算给定值的校验和并将其存储在条目中。
    /// 校验和用于后续的数据完整性检查，确保数据未被意外修改。
    /// 键和值则直接存储在条目中，以便于快速访问和操作。
    pub(crate) fn new_entry(key: Key, value: Value) -> Self {
        let check_sum = CRC32.checksum(&value);
        Self {
            check_sum,
            key,
            value: Some(value),
        }
    }

    /// 创建一个新的墓碑对象
    ///
    /// # 参数
    /// - `key`: 关联的键值对中的键
    ///
    /// # 返回值
    /// 返回一个初始化的墓碑对象，该对象包含一个键，但没有关联的价值信息
    ///
    /// # 说明
    /// 此函数用于在键值存储的上下文中表示一个已删除的键值对，
    /// 其中`check_sum`初始化为0，表示尚未计算校验和，
    /// `value`初始化为`None`，表示该墓碑对象不指向任何价值信息
    pub(crate) fn new_tombstone(key: Key) -> Self {
        let check_sum = 0;
        Self {
            check_sum,
            key,
            value: None,
        }
    }
    
    /// 检查当前对象是否为“墓碑”对象。
    ///
    /// “墓碑”对象表示一个已删除或不再存在的实体。该方法通过检查`value`字段是否为`None`来判断对象是否为“墓碑”对象。
    /// 如果`value`为`None`，则返回`true`，表示对象是“墓碑”对象；否则返回`false`。
    pub(crate) fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    /// 检查数据包是否有效。
    ///
    /// 有效性通过检查数据包的校验和与CRC32校验和是否相等来确定。
    /// 如果数据包的值存在，则进行校验和比较；如果值不存在（为None），则认为数据包有效。
    fn is_valid(&self) -> bool {
        if let Some(value) = &self.value {
            self.check_sum == CRC32.checksum(value)
        } else {
            true
        }
    }

    /// 返回校验和的字节大小
    ///
    /// # 返回值
    /// - `ByteSize`：校验和的字节大小
    ///
    /// # 说明
    /// 此函数用于指示校验和（checksum）的数据长度
    /// 了解这一信息有助于在处理或验证校验和时正确地解析数据
    const fn check_sum_byte_size() -> ByteSize {
        4
    }

    /// 获取密钥的字节大小
    ///
    /// # 返回
    /// 返回密钥的长度（以字节为单位）
    fn key_byte_size(&self) -> ByteSize {
        self.key.len() as u64
    }

    /// 计算值的字节大小
    ///
    /// 返回自我引用的值作为字节大小（如果存在），否则返回0
    pub(crate) fn value_byte_size(&self) -> ByteSize {
        self.value.as_ref().map(|v| v.len() as u64).unwrap_or(0)
    }
    
    /// 计算字节大小的常量函数
    ///
    /// # 返回值
    /// 返回字节大小，单位为字节
    ///
    /// # 说明
    /// 本函数将字位大小转换为字节大小，即字位大小除以8
    const fn size_byte_len() -> ByteSize {
        ByteSize::BITS as u64 / 8
    }
    
    /// 计算值的字节偏移量
    ///
    /// 该方法用于计算特定键关联的值在存储中的字节偏移量。计算基于校验和的字节大小、
    /// 两个键值对大小的字节数，以及键本身的字节大小。
    ///
    /// # 返回值
    /// - 返回值是`ByteOffset`类型，表示值在存储中的字节偏移量。
    pub(crate) fn value_byte_offset(&self) -> ByteOffset {
        Self::check_sum_byte_size() + Self::size_byte_len() * 2 + self.key_byte_size()
    }
    
    /// 计算对象的总字节大小
    ///
    /// 该方法用于计算对象在内存中的总字节大小，包括校验和的大小、
    /// 键的大小以及值的大小。这是通过累加各个部分的字节大小实现的。
    ///
    /// # 返回值
    ///
    /// 返回对象的总字节大小。
    pub(crate) fn total_byte_size(&self) -> ByteSize {
        // 计算校验和的字节大小
        Self::check_sum_byte_size()
        // 计算大小字节的长度，并乘以2，因为通常包含两个部分
        + Self::size_byte_len() * 2
        // 计算键的字节大小
        + self.key_byte_size()
        // 计算值的字节大小
        + self.value_byte_size()
    }
}

/// Disk layout
///  - Checksum (4 bytes long)
///  - Size of key in bytes (8 bytes long)
///  - Size of value in bytes (8 bytes long)
///  - Key
///  - Value (if tombstone, then value is None, and value size is 0)
impl Serialize for DiskLogEntry {
    /// 序列化方法，用于将当前的DiskLogEntry实例写入到一个可写入的缓冲区中。
    /// 该方法会首先写入校验和，然后是键和值的大小，最后是键和值本身。
    ///
    /// # 参数
    /// - `buf`: 一个可写入的缓冲区，实现了Write trait。
    ///
    /// # 返回值
    /// - `Result<(), BitCaskError>`: 表示操作的成功或失败，以及可能的错误信息。
    ///
    /// # 可能的错误
    /// - 如果在写入过程中发生错误，将返回BitCaskError。
    fn serialize<T: Write>(&self, buf: &mut T) -> Result<(), BitCaskError> {
       
        // 解构DiskLogEntry，以便分别处理其属性。
        let DiskLogEntry {
            check_sum,
            key,
            value,
        } = self;

        // 写入校验和。校验和用于确保数据的完整性。
        buf.write_all(&check_sum.to_be_bytes())?;

        // 计算键和值的大小，准备写入。
        let key_size = self.key_byte_size();
        let value_size = self.value_byte_size();

        // 写入键和值的大小。这允许在读取时知道键和值分别占用多少字节。
        buf.write_all(&key_size.to_be_bytes())?;
        buf.write_all(&value_size.to_be_bytes())?;

        // 写入键。键是必须的，因此直接写入。
        buf.write_all(key.as_ref())?;

        // 如果值存在，则写入值。值可能是None，因此需要检查。
        if let Some(value) = value {
            buf.write_all(value.as_ref())?;
        }

        // 序列化完成，返回Ok(())。
        Ok(())
    }
}

impl Deserialize for DiskLogEntry {
    
    /// 从可读取的缓冲区中反序列化数据
    ///
    /// # 参数
    /// - `buf`: 一个可读取的缓冲区，用于读取数据
    ///
    /// # 返回值
    /// - `Result<Self, BitCaskError>`: 表示反序列化结果的`Result`类型，
    ///   成功时返回反序列化的`Self`实例，失败时返回`BitCaskError`错误
    fn deserialize<T: Read>(buf: &mut T) -> Result<Self, BitCaskError> {
        
        // 4字节用于存储校验和
        let mut check_sum_buf = [0u8; Self::check_sum_byte_size() as usize];
        buf.read_exact(&mut check_sum_buf)?;
        let check_sum = u32::from_be_bytes(check_sum_buf);

        // 8字节用于存储大小
        let mut size_buf = [0u8; Self::size_byte_len() as usize];
        buf.read_exact(&mut size_buf)?;
        let key_size = ByteSize::from_be_bytes(size_buf);

        buf.read_exact(&mut size_buf)?;
        let value_size = ByteSize::from_be_bytes(size_buf);

        // 读取key
        let mut key_buf = vec![0 as u8; key_size as usize];
        buf.read_exact(&mut key_buf)?;
        let key = key_buf;

        // 如果是墓碑（tombstone），则value为None
        let value = if value_size > 0 {
            let mut value_buf = vec![0 as u8; value_size as usize];
            buf.read_exact(&mut value_buf)?;
            Some(value_buf)
        } else {
            None
        };

        // 构建DiskLogEntry实例
        let entry = Self {
            check_sum,
            key,
            value,
        };

        // 验证校验和
        if entry.is_valid() {
            Ok(entry)
        } else {
            Err(BitCaskError::CorruptedData("invalid checksum".to_string()))
        }
    }
}
