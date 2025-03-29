package com.romibuzi.parquetdiff.models;

import org.apache.hadoop.fs.Path;

public record ParquetDetails(Path path, long numRows, ParquetSchemaNode schemaNode, ParquetPartitions partitions) {
    public ParquetDetails(Path path, long numRows, ParquetSchemaNode schemaNode) {
        this(path, numRows, schemaNode, ParquetPartitions.fromPath(path));
    }
}
