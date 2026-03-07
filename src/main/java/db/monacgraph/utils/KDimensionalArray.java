package db.monacgraph.utils;

import java.util.Arrays;
import java.util.List;

/**
 * K-dimensional boolean array data structure
 * Supports dynamic dimension count (k) with each dimension having size n (index range: 0 to n-1)
 * Only stores boolean type data
 * Supports element operations with two coordinate formats: List<Integer> and int[]
 */
public class KDimensionalArray {
    private final int n;  // Size of each dimension
    private final int k;  // Number of dimensions
    private final boolean[] data;  // Underlying 1D boolean array for data storage
    private final int totalSize;  // Total number of elements = n^k

    /**
     * Constructor for K-dimensional boolean array
     *
     * @param n Size of each dimension (index range: 0 ~ n-1)
     * @param k Number of dimensions
     * @throws IllegalArgumentException if n or k is less than 0
     */
    public KDimensionalArray(int n, int k) {
        if (n < 0 || k < 0) {
            throw new IllegalArgumentException("n and k must be greater than 0");
        }

        this.n = n;
        this.k = k;
        this.totalSize = (int) Math.pow(n, k);
        this.data = new boolean[totalSize]; // Use boolean array with default value false
    }

    /**
     * Converts k-dimensional coordinate array to 1D array index (int[] version)
     * Formula: index = i0 * n^(k-1) + i1 * n^(k-2) + ... + i(k-1)
     *
     * @param indices k-dimensional coordinate array
     * @return Corresponding index in 1D array
     * @throws IllegalArgumentException if indices is null or length does not match k
     * @throws IndexOutOfBoundsException if any coordinate is out of [0, n-1] range
     */
    private int coordinateToIndex(int[] indices) {
        // Validate non-null and length match
        if (indices == null || indices.length != k) {
            throw new IllegalArgumentException("Coordinate array length must equal k=" + k);
        }

        int index = 0;
        int multiplier = 1;

        // Calculate from last dimension (reverse order)
        for (int i = k - 1; i >= 0; i--) {
            int coordinate = indices[i];

            // Validate coordinate range
            if (coordinate < 0 || coordinate >= n) {
                throw new IndexOutOfBoundsException(
                        "Coordinate[" + i + "]=" + coordinate + " is out of range [0, " + (n-1) + "]"
                );
            }

            index += coordinate * multiplier;
            multiplier *= n;
        }

        return index;
    }

    /**
     * Converts k-dimensional coordinate list to 1D array index (List version, keeps original logic)
     *
     * @param indices k-dimensional coordinate list
     * @return Corresponding index in 1D array
     * @throws IllegalArgumentException if indices is null or size does not match k
     * @throws IndexOutOfBoundsException if any coordinate is out of [0, n-1] range
     */
    private int coordinateToIndex(List<Integer> indices) {
        // Validate parameters
        if (indices == null || indices.size() != k) {
            throw new IllegalArgumentException("Coordinate list size must equal k=" + k);
        }

        int index = 0;
        int multiplier = 1;

        // Calculate from last dimension (reverse order)
        for (int i = k - 1; i >= 0; i--) {
            int coordinate = indices.get(i);

            // Validate coordinate range
            if (coordinate < 0 || coordinate >= n) {
                throw new IndexOutOfBoundsException(
                        "Coordinate[" + i + "]=" + coordinate + " is out of range [0, " + (n-1) + "]"
                );
            }

            index += coordinate * multiplier;
            multiplier *= n;
        }

        return index;
    }

    // ==================== Added int[] version methods ====================
    /**
     * Gets the boolean value at specified coordinates (int[] array version)
     *
     * @param indices k-dimensional coordinate array (length must equal k)
     * @return Boolean value at the specified position
     * @throws IllegalArgumentException if indices is null or length does not match k
     * @throws IndexOutOfBoundsException if any coordinate is out of [0, n-1] range
     */
    public boolean get(int[] indices) {
        int index = coordinateToIndex(indices);
        return data[index];
    }

    /**
     * Sets the boolean value at specified coordinates (int[] array version)
     *
     * @param indices k-dimensional coordinate array (length must equal k)
     * @param value Boolean value to set
     * @throws IllegalArgumentException if indices is null or length does not match k
     * @throws IndexOutOfBoundsException if any coordinate is out of [0, n-1] range
     */
    public void set(int[] indices, boolean value) {
        int index = coordinateToIndex(indices);
        data[index] = value;
    }

    // ==================== Preserved original List version methods ====================
    /**
     * Gets the boolean value at specified coordinates (List<Integer> version)
     *
     * @param indices k-dimensional coordinate list
     * @return Boolean value at the specified position
     * @throws IllegalArgumentException if indices is null or size does not match k
     * @throws IndexOutOfBoundsException if any coordinate is out of [0, n-1] range
     */
    public boolean get(List<Integer> indices) {
        int index = coordinateToIndex(indices);
        return data[index];
    }

    /**
     * Sets the boolean value at specified coordinates (List<Integer> version)
     *
     * @param indices k-dimensional coordinate list
     * @param value Boolean value to set
     * @throws IllegalArgumentException if indices is null or size does not match k
     * @throws IndexOutOfBoundsException if any coordinate is out of [0, n-1] range
     */
    public void set(List<Integer> indices, boolean value) {
        int index = coordinateToIndex(indices);
        data[index] = value;
    }

    // ==================== Other original methods ====================
    /**
     * Gets the number of dimensions
     *
     * @return Number of dimensions (k)
     */
    public int getDimensions() {
        return k;
    }

    /**
     * Gets the size of each dimension
     *
     * @return Size of each dimension (n)
     */
    public int getSizePerDimension() {
        return n;
    }

    /**
     * Gets the total number of elements in the array
     *
     * @return Total elements count (n^k)
     */
    public int getTotalSize() {
        return totalSize;
    }

    /**
     * Clears the array (sets all elements to false)
     * Boolean array default value is already false, explicit reset ensures semantic clarity
     */
    public void clear() {
        Arrays.fill(data, false);
    }

    /**
     * Fills the array with specified boolean value
     *
     * @param value Boolean value to fill the array with
     */
    public void fill(boolean value) {
        Arrays.fill(data, value);
    }

    /**
     * Returns string representation of the k-dimensional array
     *
     * @return Formatted string with n, k and totalSize
     */
    @Override
    public String toString() {
        return String.format("KDimensionalArray[n=%d, k=%d, totalSize=%d]", n, k, totalSize);
    }
}