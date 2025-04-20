package io.github.romibuzi.parquetdiff.metadata;

/**
 * Represents a single partition.
 * @param key   The partition key.
 * @param value The partition value.
 */
public record ParquetPartition(String key, String value) {
    /**
     * Parses a partition represented as "key=value".
     *
     * @param partition The input partition.
     * @return A {@link ParquetPartition} instance.
     */
    static ParquetPartition fromString(String partition) {
        String[] splits = partition.split("=", 2);
        return new ParquetPartition(splits[0], splits[1]);
    }

    @Override
    public String toString() {
        return key + "=" + value;
    }
}
