package com.romibuzi.parquetdiff.models;

public record ParquetPartition(String key, String value) {
    public static ParquetPartition fromString(String partition) {
        String[] splits = partition.split("=", 2);
        return new ParquetPartition(splits[0], splits[1]);
    }
}
