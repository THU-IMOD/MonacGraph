#[cfg(test)]
mod test_property {
    use crate::property::{EdgePropertyKey, VertexPropertyKey};

    #[test]
    fn test_vertex_property_key_encode_decode() {
        let key = VertexPropertyKey::new(12345, "age".to_string());
        let encoded = key.encode();
        let decoded = VertexPropertyKey::decode(&encoded).unwrap();

        assert_eq!(key, decoded);
        assert_eq!(decoded.property_name, "age");
        assert_eq!(decoded.vertex_id, 12345);
    }

    #[test]
    fn test_edge_property_key_encode_decode() {
        let key = EdgePropertyKey::new(100, 200, "weight".to_string());
        let encoded = key.encode();
        let decoded = EdgePropertyKey::decode(&encoded).unwrap();

        assert_eq!(key, decoded);
        assert_eq!(decoded.property_name, "weight");
        assert_eq!(decoded.source_id, 100);
        assert_eq!(decoded.destination_id, 200);
    }

    #[test]
    fn test_vertex_property_prefix() {
        let prefix = VertexPropertyKey::prefix(12345);

        let key1 = VertexPropertyKey::new(12345, "age".to_string()).encode();
        let key2 = VertexPropertyKey::new(12345, "name".to_string()).encode();
        let key3 = VertexPropertyKey::new(99999, "age".to_string()).encode();

        assert!(key1.starts_with(&prefix));
        assert!(key2.starts_with(&prefix));
        assert!(!key3.starts_with(&prefix));
    }

    #[test]
    fn test_edge_property_prefix() {
        let prefix = EdgePropertyKey::prefix(100, 200);

        let key1 = EdgePropertyKey::new(100, 200, "weight".to_string()).encode();
        let key2 = EdgePropertyKey::new(100, 200, "label".to_string()).encode();
        let key3 = EdgePropertyKey::new(100, 999, "weight".to_string()).encode();

        assert!(key1.starts_with(&prefix));
        assert!(key2.starts_with(&prefix));
        assert!(!key3.starts_with(&prefix));
    }

    #[test]
    fn test_lexicographic_ordering() {
        // Vertex keys ordered by vid, then property name
        let key1 = VertexPropertyKey::new(100, "aaa".to_string()).encode();
        let key2 = VertexPropertyKey::new(100, "zzz".to_string()).encode();
        let key3 = VertexPropertyKey::new(200, "aaa".to_string()).encode();

        assert!(key1 < key2);
        assert!(key2 < key3);

        // Edge keys ordered by src, dst, then property name
        let ekey1 = EdgePropertyKey::new(100, 200, "aaa".to_string()).encode();
        let ekey2 = EdgePropertyKey::new(100, 200, "zzz".to_string()).encode();
        let ekey3 = EdgePropertyKey::new(100, 300, "aaa".to_string()).encode();
        let ekey4 = EdgePropertyKey::new(200, 100, "aaa".to_string()).encode();

        assert!(ekey1 < ekey2);
        assert!(ekey2 < ekey3);
        assert!(ekey3 < ekey4);
    }
}
