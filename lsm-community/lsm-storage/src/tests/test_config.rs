#[cfg(test)]
mod test_config {
    use crate::config::{ConfigManager, LsmCommunityStorageOptions};

    #[test]
    fn test_load_default_config() {
        let config = LsmCommunityStorageOptions::default();
        assert_eq!(config.block_size, 1024 * 4);
        assert_eq!(config.num_mem_graph_limit, 3);
    }

    #[test]
    fn test_save_and_load_config() {
        let test_path = "test_config.yaml";

        // Create and save config
        let original_config = LsmCommunityStorageOptions {
            block_size: 8192,
            graph_name: "test_graph".to_string(),
            ..Default::default()
        };

        ConfigManager::save_to_yaml(&original_config, test_path).unwrap();

        // Load config
        let loaded_config: LsmCommunityStorageOptions =
            ConfigManager::load_from_yaml(test_path).unwrap();

        assert_eq!(loaded_config.block_size, 8192);
        assert_eq!(loaded_config.graph_name, "test_graph");

        // Cleanup
        std::fs::remove_file(test_path).ok();
    }

    #[test]
    fn test_load_example_config() {
        let test_path = "../config/example.yaml";

        // Load config
        let loaded_config: LsmCommunityStorageOptions =
            ConfigManager::load_from_yaml(test_path).unwrap();

        assert_eq!(loaded_config.block_size, 4096);
        assert_eq!(loaded_config.graph_name, "example");
    }
}
