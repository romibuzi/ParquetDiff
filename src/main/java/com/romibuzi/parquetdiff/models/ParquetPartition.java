package com.romibuzi.parquetdiff.models;

public record ParquetPartition(String key, String value) {
    /**
     * Parses a partition represented as "key=value".
     *
     * @param partition The input partition.
     * @return A {@link ParquetPartition} instance.
     */
    public static ParquetPartition fromString(String partition) {
        String[] splits = partition.split("=", 2);
        return new ParquetPartition(splits[0], splits[1]);
    }

    @Override
    public String toString() {
        return key + "=" + value;
    }
}
