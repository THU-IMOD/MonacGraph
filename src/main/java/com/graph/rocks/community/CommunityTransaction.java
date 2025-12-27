package com.graph.rocks.community;

import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import java.util.function.Consumer;

/**
 * Transaction implementation for LSM-Community graph storage
 * This implementation explicitly does NOT support transactions
 * Compliant with TinkerPop Transaction interface requirements
 */
@SuppressWarnings("unused")
public class CommunityTransaction implements Transaction {

    /**
     * Open a new transaction (not supported)
     * @throws UnsupportedOperationException Always thrown (transactions not supported)
     */
    @Override
    public void open() {
        throw new UnsupportedOperationException("RocksDB does not support transactions");
    }

    /**
     * Commit the current transaction (not supported)
     * @throws UnsupportedOperationException Always thrown (transactions not supported)
     */
    @Override
    public void commit() {
        throw new UnsupportedOperationException("RocksDB does not support transactions");
    }

    /**
     * Rollback the current transaction (not supported)
     * @throws UnsupportedOperationException Always thrown (transactions not supported)
     */
    @Override
    public void rollback() {
        throw new UnsupportedOperationException("RocksDB does not support transactions");
    }

    /**
     * Begin a transactional traversal (not supported)
     * @param traversalSourceClass Traversal source class
     * @param <T> Traversal source type
     * @return Never returns (throws exception)
     * @throws UnsupportedOperationException Always thrown (transactions not supported)
     */
    @Override
    public <T extends TraversalSource> T begin(Class<T> traversalSourceClass) {
        throw new UnsupportedOperationException("RocksDB does not support transactions");
    }

    /**
     * Check if transaction is open (always false for this implementation)
     * @return Always returns false (no transaction support)
     */
    @Override
    public boolean isOpen() {
        return false;
    }

    /**
     * Mark transaction as read-write (no-op for non-transactional implementation)
     */
    @Override
    public void readWrite() {
        // No operation - read/write mode not applicable to non-transactional storage
    }

    /**
     * Close the transaction (no-op for non-transactional implementation)
     */
    @Override
    public void close() {
        // No operation - no transaction state to clean up
    }

    /**
     * Configure read-write transaction listener (ignored)
     * @param consumer Listener consumer
     * @return This transaction instance (unchanged)
     */
    @Override
    public Transaction onReadWrite(Consumer<Transaction> consumer) {
        return this; // Listener configuration ignored for non-transactional implementation
    }

    /**
     * Configure close transaction listener (ignored)
     * @param consumer Listener consumer
     * @return This transaction instance (unchanged)
     */
    @Override
    public Transaction onClose(Consumer<Transaction> consumer) {
        return this; // Listener configuration ignored for non-transactional implementation
    }

    /**
     * Add transaction status listener (no-op for non-transactional implementation)
     * @param listener Status listener consumer
     */
    @Override
    public void addTransactionListener(Consumer<Status> listener) {
        // No operation - transaction events not supported
    }

    /**
     * Remove transaction status listener (no-op for non-transactional implementation)
     * @param listener Status listener consumer
     */
    @Override
    public void removeTransactionListener(Consumer<Status> listener) {
        // No operation - transaction events not supported
    }

    /**
     * Clear all transaction listeners (no-op for non-transactional implementation)
     */
    @Override
    public void clearTransactionListeners() {
        // No operation - no listeners to clear
    }
}