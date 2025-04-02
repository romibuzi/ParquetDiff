package com.romibuzi.parquetdiff.models;

import org.apache.hadoop.fs.Path;

public record ParquetDetails(Path path, long numRows, ParquetSchemaNode schema, ParquetPartitions partitions) {
    public ParquetDetails(Path path, long numRows, ParquetSchemaNode schema) {
        this(path, numRows, schema, ParquetPartitions.fromPath(path));
    }
}
