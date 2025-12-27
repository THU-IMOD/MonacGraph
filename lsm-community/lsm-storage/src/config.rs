use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, Write};
use std::path::Path;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LsmCommunityStorageOptions {
    // Block size in bytes
    #[serde(default = "default_block_size")]
    pub block_size: usize,

    // SST size in bytes, also the approximate memtable capacity limit
    #[serde(default = "default_min_bucket_size")]
    pub min_bucket_size: usize,

    // Maximum number of MemGraphs in memory, flush to L0 when exceeding this limit
    #[serde(default = "default_num_mem_graph_limit")]
    pub num_mem_graph_limit: usize,

    // The boundary of a giant vertex
    #[serde(default = "default_giant_vertex_boundary")]
    pub giant_vertex_boundary: usize,

    // Graph name
    #[serde(default)]
    pub graph_name: String,

    // Workspace Directory
    #[serde(default = "default_work_space_dir")]
    pub work_space_dir: String,

    // Cache capacity
    #[serde(default = "default_block_cache_capacity")]
    pub block_cache_capacity: u64,

    // Giant cache capacity
    #[serde(default = "default_giant_cache_capacity")]
    pub giant_cache_capacity: u64,
}

// Default value functions for serde
fn default_block_size() -> usize {
    1024 * 4
}
fn default_min_bucket_size() -> usize {
    1024 * 1024 * 8
}
fn default_num_mem_graph_limit() -> usize {
    3
}
fn default_giant_vertex_boundary() -> usize {
    128
}
fn default_work_space_dir() -> String {
    "workspace".to_owned()
}
fn default_block_cache_capacity() -> u64 {
    1 << 20
}
fn default_giant_cache_capacity() -> u64 {
    10_000
}

impl Default for LsmCommunityStorageOptions {
    fn default() -> Self {
        LsmCommunityStorageOptions {
            block_size: default_block_size(),
            min_bucket_size: default_min_bucket_size(),
            num_mem_graph_limit: default_num_mem_graph_limit(),
            giant_vertex_boundary: default_giant_vertex_boundary(),
            graph_name: String::new(),
            work_space_dir: default_work_space_dir(),
            block_cache_capacity: default_block_cache_capacity(),
            giant_cache_capacity: default_giant_cache_capacity(),
        }
    }
}

/// Configuration manager for loading and saving YAML config files
pub struct ConfigManager;

impl ConfigManager {
    /// Load configuration from a YAML file
    ///
    /// # Arguments
    /// * `path` - Path to the YAML configuration file
    ///
    /// # Returns
    /// * `Result<T, ConfigError>` - Parsed configuration or error
    pub fn load_from_yaml<T, P>(path: P) -> Result<T, ConfigError>
    where
        T: for<'de> Deserialize<'de> + Default,
        P: AsRef<Path>,
    {
        let path = path.as_ref();

        // Check if file exists
        if !path.exists() {
            return Err(ConfigError::FileNotFound(
                path.to_string_lossy().to_string(),
            ));
        }

        // Open and read the file
        let file = File::open(path).map_err(|e| ConfigError::IoError(e))?;

        let reader = BufReader::new(file);

        // Parse YAML with default values for missing fields
        let config: T =
            serde_yaml::from_reader(reader).map_err(|e| ConfigError::ParseError(e.to_string()))?;

        Ok(config)
    }

    /// Load configuration from YAML file, or return default if file doesn't exist
    ///
    /// # Arguments
    /// * `path` - Path to the YAML configuration file
    ///
    /// # Returns
    /// * `T` - Parsed configuration or default configuration
    pub fn load_or_default<T, P>(path: P) -> T
    where
        T: for<'de> Deserialize<'de> + Default,
        P: AsRef<Path>,
    {
        Self::load_from_yaml(path).unwrap_or_else(|e| {
            eprintln!("Warning: Failed to load config: {}. Using default.", e);
            T::default()
        })
    }

    /// Save configuration to a YAML file
    ///
    /// # Arguments
    /// * `config` - Configuration to save
    /// * `path` - Path where to save the YAML file
    ///
    /// # Returns
    /// * `Result<(), ConfigError>` - Success or error
    pub fn save_to_yaml<T, P>(config: &T, path: P) -> Result<(), ConfigError>
    where
        T: Serialize,
        P: AsRef<Path>,
    {
        let yaml_string = serde_yaml::to_string(config)
            .map_err(|e| ConfigError::SerializeError(e.to_string()))?;

        let mut file = File::create(path.as_ref()).map_err(|e| ConfigError::IoError(e))?;

        file.write_all(yaml_string.as_bytes())
            .map_err(|e| ConfigError::IoError(e))?;

        Ok(())
    }

    /// Generate a default configuration file if it doesn't exist
    ///
    /// # Arguments
    /// * `path` - Path where to create the config file
    ///
    /// # Returns
    /// * `Result<(), ConfigError>` - Success or error
    pub fn create_default_config<T, P>(path: P) -> Result<(), ConfigError>
    where
        T: Serialize + Default,
        P: AsRef<Path>,
    {
        let path = path.as_ref();

        if path.exists() {
            return Err(ConfigError::FileAlreadyExists(
                path.to_string_lossy().to_string(),
            ));
        }

        let default_config = T::default();
        Self::save_to_yaml(&default_config, path)
    }
}

/// Configuration error types
#[derive(Debug)]
pub enum ConfigError {
    /// File not found
    FileNotFound(String),
    /// File already exists
    FileAlreadyExists(String),
    /// I/O error
    IoError(std::io::Error),
    /// YAML parse error
    ParseError(String),
    /// Serialize error
    SerializeError(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::FileNotFound(path) => write!(f, "Config file not found: {}", path),
            ConfigError::FileAlreadyExists(path) => {
                write!(f, "Config file already exists: {}", path)
            }
            ConfigError::IoError(e) => write!(f, "I/O error: {}", e),
            ConfigError::ParseError(e) => write!(f, "Parse error: {}", e),
            ConfigError::SerializeError(e) => write!(f, "Serialize error: {}", e),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigError::IoError(e) => Some(e),
            _ => None,
        }
    }
}
