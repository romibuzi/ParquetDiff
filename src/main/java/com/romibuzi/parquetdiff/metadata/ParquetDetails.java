package com.romibuzi.parquetdiff.metadata;

import org.apache.hadoop.fs.Path;

/**
 * Contains information about a single Parquet file.
 *
 * @param path       The complete Path of the file.
 * @param numRows    Number of rows in the file.
 * @param schema     Schema of the file.
 * @param partitions Extracted partitions from file path.
 */
public record ParquetDetails(Path path, long numRows, ParquetSchemaNode schema, ParquetPartitions partitions) {
    public ParquetDetails(Path path, long numRows, ParquetSchemaNode schema) {
        this(path, numRows, schema, ParquetPartitions.fromPath(path));
    }

    public void printSchema() {
        schema.print();
    }
}
