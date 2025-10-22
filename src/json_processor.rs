use regex::Regex;
use serde_json::Value;
use std::fs;

/// JSON 处理错误类型
#[derive(Debug)]
pub enum JsonProcessorError {
    FileError(std::io::Error),
    JsonError(serde_json::Error),
    TemplateError(String),
}

impl std::fmt::Display for JsonProcessorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonProcessorError::FileError(e) => write!(f, "文件错误: {}", e),
            JsonProcessorError::JsonError(e) => write!(f, "JSON 错误: {}", e),
            JsonProcessorError::TemplateError(e) => write!(f, "模板错误: {}", e),
        }
    }
}

impl std::error::Error for JsonProcessorError {}

impl From<std::io::Error> for JsonProcessorError {
    fn from(error: std::io::Error) -> Self {
        JsonProcessorError::FileError(error)
    }
}

impl From<serde_json::Error> for JsonProcessorError {
    fn from(error: serde_json::Error) -> Self {
        JsonProcessorError::JsonError(error)
    }
}

/// JSON 处理服务
pub struct JsonProcessor;

impl JsonProcessor {
    /// 从文件读取 JSON 内容
    pub fn read_json_file(file_path: &str) -> Result<String, JsonProcessorError> {
        fs::read_to_string(file_path).map_err(JsonProcessorError::FileError)
    }

    /// 保存 JSON 内容到文件
    pub fn save_json_file(file_path: &str, content: &str) -> Result<(), JsonProcessorError> {
        fs::write(file_path, content).map_err(JsonProcessorError::FileError)
    }

    /// 创建示例 JSON 文件
    pub fn create_example_json_file(file_path: &str) -> Result<(), JsonProcessorError> {
        let example_json = r#"{
    "title": "从文件读取的 POST 数据",
    "body": "这是一个从 JSON 文件读取数据的示例",
    "userId": 1,
    "tags": ["rust", "http", "post"],
    "metadata": {
        "priority": "high",
        "category": "development"
    }
}"#;
        Self::save_json_file(file_path, example_json)
    }

    /// 解析 JSON 字符串为 Value
    pub fn parse_json(json_str: &str) -> Result<Value, JsonProcessorError> {
        serde_json::from_str(json_str).map_err(JsonProcessorError::JsonError)
    }

    /// 将 Value 格式化为漂亮的 JSON 字符串
    pub fn format_json_pretty(value: &Value) -> Result<String, JsonProcessorError> {
        serde_json::to_string_pretty(value).map_err(JsonProcessorError::JsonError)
    }

    /// 修改 JSON 字段值
    pub fn modify_json_field(
        value: &mut Value,
        field_path: &str,
        new_value: Value,
    ) -> Result<(), JsonProcessorError> {
        let parts: Vec<&str> = field_path.split('.').collect();
        Self::modify_nested_field(value, &parts, new_value)
    }

    /// 递归修改嵌套字段
    fn modify_nested_field(
        value: &mut Value,
        path_parts: &[&str],
        new_value: Value,
    ) -> Result<(), JsonProcessorError> {
        if path_parts.is_empty() {
            return Ok(());
        }

        let current_part = path_parts[0];
        let remaining_parts = &path_parts[1..];

        if remaining_parts.is_empty() {
            // 到达目标字段
            match value {
                Value::Object(map) => {
                    map.insert(current_part.to_string(), new_value);
                    Ok(())
                }
                Value::Array(arr) => {
                    // 处理数组索引
                    if let Ok(index) = current_part.parse::<usize>() {
                        if index < arr.len() {
                            arr[index] = new_value;
                            Ok(())
                        } else {
                            Err(JsonProcessorError::TemplateError(format!(
                                "数组索引 {} 超出范围 (数组长度: {})",
                                index,
                                arr.len()
                            )))
                        }
                    } else {
                        Err(JsonProcessorError::TemplateError(format!(
                            "无效的数组索引: {}",
                            current_part
                        )))
                    }
                }
                _ => Err(JsonProcessorError::TemplateError(
                    "目标不是对象或数组，无法修改字段".to_string(),
                )),
            }
        } else {
            // 继续深入嵌套结构
            match value {
                Value::Object(map) => {
                    if let Some(next_value) = map.get_mut(current_part) {
                        Self::modify_nested_field(next_value, remaining_parts, new_value)
                    } else {
                        // 检查下一部分是否是数组索引
                        let next_part = remaining_parts[0];
                        if let Ok(_) = next_part.parse::<usize>() {
                            // 下一部分是数组索引，创建数组
                            let new_array = Value::Array(Vec::new());
                            map.insert(current_part.to_string(), new_array);
                        } else {
                            // 下一部分是对象字段，创建对象
                            let new_map = serde_json::Map::new();
                            map.insert(current_part.to_string(), Value::Object(new_map));
                        }

                        if let Some(next_value) = map.get_mut(current_part) {
                            Self::modify_nested_field(next_value, remaining_parts, new_value)
                        } else {
                            Err(JsonProcessorError::TemplateError(
                                "无法创建中间结构".to_string(),
                            ))
                        }
                    }
                }
                Value::Array(arr) => {
                    // 处理数组中的对象
                    if let Ok(index) = current_part.parse::<usize>() {
                        if index < arr.len() {
                            Self::modify_nested_field(&mut arr[index], remaining_parts, new_value)
                        } else {
                            Err(JsonProcessorError::TemplateError(format!(
                                "数组索引 {} 超出范围 (数组长度: {})",
                                index,
                                arr.len()
                            )))
                        }
                    } else {
                        Err(JsonProcessorError::TemplateError(format!(
                            "无效的数组索引: {}",
                            current_part
                        )))
                    }
                }
                _ => Err(JsonProcessorError::TemplateError(
                    "当前路径不是对象或数组".to_string(),
                )),
            }
        }
    }

    /// 模板替换功能
    pub fn template_replace(template: &str, replacements: &[(&str, &str)]) -> String {
        let mut result = template.to_string();

        for (placeholder, value) in replacements {
            result = result.replace(&format!("{{{{{}}}}}", placeholder), value);
        }

        result
    }

    /// 从 JSON 中提取请求 ID
    pub fn extract_request_id(json_data: &str) -> Option<usize> {
        // 尝试直接解析 JSON
        if let Ok(value) = Self::parse_json(json_data) {
            if let Some(Value::Number(id)) = value.get("request_id") {
                return id.as_u64().map(|id| id as usize);
            }
        }

        // 使用正则表达式匹配
        let re = Regex::new(r#""request_id"\s*:\s*(\d+)"#).ok()?;
        if let Some(caps) = re.captures(json_data) {
            if let Ok(id) = caps[1].parse::<usize>() {
                return Some(id);
            }
        }

        None
    }

    /// 批量修改 JSON 字段
    pub fn batch_modify_fields(
        value: &mut Value,
        modifications: &[(&str, Value)],
    ) -> Result<(), JsonProcessorError> {
        for (field_path, new_value) in modifications {
            Self::modify_json_field(value, field_path, new_value.clone())?;
        }
        Ok(())
    }

    /// 验证 JSON 格式
    pub fn validate_json(json_str: &str) -> Result<(), JsonProcessorError> {
        Self::parse_json(json_str)?;
        Ok(())
    }
}

/// 简化的 JSON 处理函数
pub fn parse_and_modify_json(
    json_str: &str,
    modifications: &[(&str, Value)],
) -> Result<String, JsonProcessorError> {
    let mut value = JsonProcessor::parse_json(json_str)?;
    JsonProcessor::batch_modify_fields(&mut value, modifications)?;
    JsonProcessor::format_json_pretty(&value)
}
