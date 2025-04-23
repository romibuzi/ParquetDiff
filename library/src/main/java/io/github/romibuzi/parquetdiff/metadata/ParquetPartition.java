package io.github.romibuzi.parquetdiff.metadata;

import java.util.Objects;

/**
 * Represents a single key value partition.
 */
public final class ParquetPartition {
    private final String key;
    private final String value;

    public ParquetPartition(String key, String value) {
        this.key = key;
        this.value = value;
    }

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

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return key + "=" + value;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParquetPartition that = (ParquetPartition) o;
        return Objects.equals(key, that.key) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }
}
