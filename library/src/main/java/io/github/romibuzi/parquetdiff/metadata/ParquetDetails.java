package io.github.romibuzi.parquetdiff.metadata;

import org.apache.hadoop.fs.Path;

/**
 * Contains information about a single Parquet file.
 *
 * @param path       The complete Path of the file.
 * @param numRows    Number of rows in the file.
 * @param schema     Schema of the file.
 * @param partitions Extracted partitions from the file Path.
 */
public record ParquetDetails(Path path, long numRows, ParquetSchemaNode schema, ParquetPartitions partitions) {
    /**
     * Creates a ParquetDetails instance where partitions will be parsed from the given path.
     */
    public ParquetDetails(Path path, long numRows, ParquetSchemaNode schema) {
        this(path, numRows, schema, ParquetPartitions.fromPath(path));
    }

    /**
     * Prints the Schema in a tree format.
     */
    public void printSchema() {
        schema.print();
    }
}
