package io.github.romibuzi.parquetdiff.metadata;

import org.apache.hadoop.fs.Path;

import java.io.PrintStream;
import java.util.Objects;

/**
 * Contains information about a single Parquet file.
 */
public final class ParquetDetails {
    private final Path path;
    private final long numRows;
    private final ParquetSchemaNode schema;
    private final ParquetPartitions partitions;

    /**
     * @param path       The complete Path of the file.
     * @param numRows    Number of rows in the file.
     * @param schema     Schema of the file.
     * @param partitions Extracted partitions from the file Path.
     */
    public ParquetDetails(Path path, long numRows, ParquetSchemaNode schema, ParquetPartitions partitions) {
        this.path = path;
        this.numRows = numRows;
        this.schema = schema;
        this.partitions = partitions;
    }

    /**
     * Creates a ParquetDetails instance where partitions will be parsed from the given path.
     */
    public ParquetDetails(Path path, long numRows, ParquetSchemaNode schema) {
        this(path, numRows, schema, ParquetPartitions.fromPath(path));
    }

    public Path getPath() {
        return path;
    }

    public long getNumRows() {
        return numRows;
    }

    public ParquetSchemaNode getSchema() {
        return schema;
    }

    public ParquetPartitions getPartitions() {
        return partitions;
    }

    /**
     * Prints the Schema in a tree format.
     *
     * @param printStream The stream to write into, ex: System.out.
     */
    public void printSchema(PrintStream printStream) {
        schema.print(printStream);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParquetDetails that = (ParquetDetails) o;
        return numRows == that.numRows && Objects.equals(path, that.path) && Objects.equals(schema, that.schema)
                && Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, numRows, schema, partitions);
    }
}
