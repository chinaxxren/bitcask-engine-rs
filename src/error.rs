use thiserror::Error;

#[derive(Debug, Error)]
/// 定义BitCask存储引擎中可能遇到的错误类型
pub enum BitCaskError {
    /// 透明地包装标准库IO错误，确保可以处理任何IO相关的问题
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    /// 当数据被损坏时抛出的错误，{0}是一个占位符，将在错误被格式化时替换为具体的错误信息
    #[error("Data is corrupted: {0}")]
    CorruptedData(String),
    /// 透明地包装anyhow::Error，用于处理无法归类为其它错误的情况
    #[error(transparent)]
    UnexpectedError(#[from] anyhow::Error),
    /// 当尝试添加一个已存在的键时抛出的错误
    #[error("Key already exists")]
    KeyExists,
    /// 当查询一个不存在的键时抛出的错误
    #[error("Key does not exist")]
    KeyNotFound,
}