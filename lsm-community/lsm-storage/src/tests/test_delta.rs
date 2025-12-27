#[cfg(test)]
mod test_delta {
    use crate::delta::{DeltaLog, DeltaOpType, DeltaOperation};

    #[test]
    fn test_delta_log_encode_decode() {
        let mut log = DeltaLog::new();
        log.add_op(DeltaOperation::new(100, DeltaOpType::AddNeighbor, 1));
        log.add_op(DeltaOperation::new(200, DeltaOpType::RemoveNeighbor, 2));
        log.add_op(DeltaOperation::new(300, DeltaOpType::AddNeighbor, 3));

        let encoded = log.encode();
        let decoded = DeltaLog::decode(&encoded).unwrap();

        assert_eq!(log.len(), decoded.len());
        for (original, decoded) in log.ops().iter().zip(decoded.ops().iter()) {
            assert_eq!(original.timestamp, decoded.timestamp);
            assert_eq!(original.neighbor, decoded.neighbor);
            assert_eq!(original.op_type, decoded.op_type);
        }
    }

    #[test]
    fn test_delta_log_merge_last_write_wins() {
        let log1 = DeltaLog::from_ops(vec![
            DeltaOperation::new(100, DeltaOpType::AddNeighbor, 1),
            DeltaOperation::new(200, DeltaOpType::AddNeighbor, 2),
        ]);

        let log2 = DeltaLog::from_ops(vec![
            DeltaOperation::new(150, DeltaOpType::RemoveNeighbor, 1), // Should win (150 > 100)
            DeltaOperation::new(250, DeltaOpType::RemoveNeighbor, 2), // Should win (250 > 200)
            DeltaOperation::new(300, DeltaOpType::AddNeighbor, 3),
        ]);

        let merged = DeltaLog::merge(&[log1, log2]);

        assert_eq!(merged.len(), 3);

        // Check neighbor 1: should keep ts=150 (larger timestamp wins)
        let op1 = merged.ops().iter().find(|op| op.neighbor == 1).unwrap();
        assert_eq!(op1.timestamp, 150);
        assert_eq!(op1.get_op_type(), Some(DeltaOpType::RemoveNeighbor));

        // Check neighbor 2: should keep ts=250 (larger timestamp wins)
        let op2 = merged.ops().iter().find(|op| op.neighbor == 2).unwrap();
        assert_eq!(op2.timestamp, 250);
        assert_eq!(op2.get_op_type(), Some(DeltaOpType::RemoveNeighbor));

        // Check neighbor 3: only one operation
        let op3 = merged.ops().iter().find(|op| op.neighbor == 3).unwrap();
        assert_eq!(op3.timestamp, 300);
        assert_eq!(op3.get_op_type(), Some(DeltaOpType::AddNeighbor));
    }

    #[test]
    fn test_merge_for_rocksdb() {
        let base_log =
            DeltaLog::from_ops(vec![DeltaOperation::new(100, DeltaOpType::AddNeighbor, 1)]);
        let base_bytes = base_log.encode();

        let operand1 =
            DeltaOperation::encode_batch(&[DeltaOperation::new(200, DeltaOpType::AddNeighbor, 2)]);

        let operand2 = DeltaOperation::encode_batch(&[
            DeltaOperation::new(150, DeltaOpType::RemoveNeighbor, 1),
            DeltaOperation::new(300, DeltaOpType::AddNeighbor, 3),
        ]);

        let merged_bytes = DeltaLog::merge_for_rocksdb(
            Some(&base_bytes),
            &[operand1.as_slice(), operand2.as_slice()],
        )
        .unwrap();

        let merged = DeltaLog::decode(&merged_bytes).unwrap();
        assert_eq!(merged.len(), 3);
    }

    #[test]
    fn test_partial_merge_for_rocksdb() {
        let operand1 =
            DeltaOperation::encode_batch(&[DeltaOperation::new(100, DeltaOpType::AddNeighbor, 1)]);

        let operand2 = DeltaOperation::encode_batch(&[
            DeltaOperation::new(200, DeltaOpType::AddNeighbor, 2),
            DeltaOperation::new(300, DeltaOpType::AddNeighbor, 3),
        ]);

        let result =
            DeltaLog::partial_merge_for_rocksdb(&[operand1.as_slice(), operand2.as_slice()])
                .unwrap();

        // Should be concatenated raw operations
        assert_eq!(result.len(), 3 * DeltaOperation::ENCODED_SIZE);

        let ops = DeltaOperation::decode_batch(&result).unwrap();
        assert_eq!(ops.len(), 3);
    }

    #[test]
    fn test_add_op_maintains_order() {
        let mut log = DeltaLog::new();
        log.add_op(DeltaOperation::new(300, DeltaOpType::AddNeighbor, 3));
        log.add_op(DeltaOperation::new(100, DeltaOpType::AddNeighbor, 1));
        log.add_op(DeltaOperation::new(200, DeltaOpType::AddNeighbor, 2));

        let ops = log.ops();
        assert_eq!(ops[0].timestamp, 100);
        assert_eq!(ops[1].timestamp, 200);
        assert_eq!(ops[2].timestamp, 300);
    }

    #[test]
    fn test_delta_operation_encode_decode() {
        let op = DeltaOperation::new(12345678, DeltaOpType::AddNeighbor, 999);
        let encoded = op.encode();
        let decoded = DeltaOperation::decode(&encoded).unwrap();

        assert_eq!(op.timestamp, decoded.timestamp);
        assert_eq!(op.neighbor, decoded.neighbor);
        assert_eq!(op.op_type, decoded.op_type);
        assert_eq!(decoded.get_op_type(), Some(DeltaOpType::AddNeighbor));
    }

    #[test]
    fn test_delta_operation_batch() {
        let ops = vec![
            DeltaOperation::new(100, DeltaOpType::AddNeighbor, 1),
            DeltaOperation::new(200, DeltaOpType::RemoveNeighbor, 2),
            DeltaOperation::new(300, DeltaOpType::AddNeighbor, 3),
        ];

        let encoded = DeltaOperation::encode_batch(&ops);
        let decoded = DeltaOperation::decode_batch(&encoded).unwrap();

        assert_eq!(ops.len(), decoded.len());
        for (original, decoded) in ops.iter().zip(decoded.iter()) {
            assert_eq!(original.timestamp, decoded.timestamp);
            assert_eq!(original.neighbor, decoded.neighbor);
            assert_eq!(original.op_type, decoded.op_type);
        }
    }

    #[test]
    fn test_invalid_op_type() {
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&100u64.to_le_bytes());
        bytes[8..12].copy_from_slice(&999u32.to_le_bytes());
        bytes[12..16].copy_from_slice(&999u32.to_le_bytes()); // Invalid op_type

        assert!(DeltaOperation::decode(&bytes).is_err());
    }

    #[test]
    fn test_encoded_size() {
        assert_eq!(DeltaOperation::ENCODED_SIZE, 16);
        assert_eq!(
            std::mem::size_of::<DeltaOperation>(),
            16,
            "DeltaOperation should be 16 bytes for alignment"
        );
    }
}
