package com.romibuzi.parquetdiff.metadata;

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
