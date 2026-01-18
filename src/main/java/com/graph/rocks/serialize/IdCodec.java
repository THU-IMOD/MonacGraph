package com.graph.rocks.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;

@SuppressWarnings("unused")
public class IdCodec {

    /**
     * Converts an Object type id to byte[]
     * Supports common types such as String, Long, Integer, UUID, etc.
     */
    public static byte[] toBytes(Object id) {
        if (id == null) {
            throw new IllegalArgumentException("ID cannot be null.");
        }

        if (id instanceof org.apache.tinkerpop.gremlin.structure.Element) {
            id = ((org.apache.tinkerpop.gremlin.structure.Element) id).id();
        }

        // Process the string
        if (id instanceof String) {
            return ((String) id).getBytes(StandardCharsets.UTF_8);
        }

        // Use Java serialization for other types (e.g., Long, Integer, etc.)
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(byteArrayOutputStream)) {
            oos.writeObject(id);
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("ID serialization failed: " + id, e);
        }
    }

    /**
     * Converts byte[] back to the original Object type
     * Completely reversible with the toBytes method
     */
    public static Object fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("byte[] cannot be null or empty.");
        }

        // Try parsing as a string first (to support String type id)
        try {
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            // For non-string types, use Java deserialization
            try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                 ObjectInputStream ois = new ObjectInputStream(bais)) {
                return ois.readObject();
            } catch (IOException | ClassNotFoundException ex) {
                throw new RuntimeException("Failed to deserialize byte[].", ex);
            }
        }
    }
}