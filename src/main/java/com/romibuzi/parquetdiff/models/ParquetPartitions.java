package com.romibuzi.parquetdiff.models;

import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.List;

public record ParquetPartitions(List<ParquetPartition> partitions) {
    /**
     * Parses the given {@link Path} to extract partitions from folders formatted as /partition=value/.
     *
     * @param path The full path to a Parquet file.
     * @return A {@link ParquetPartitions} instance with extracted partitions.
     */
    public static ParquetPartitions fromPath(Path path) {
        List<ParquetPartition> partitions =
                Arrays.stream(path.toUri().getPath().split("/"))
                        .filter(part -> part.contains("="))
                        .map(ParquetPartition::fromString)
                        .toList();

        return new ParquetPartitions(partitions);
    }

    /**
     * @return All partition keys.
     */
    public List<String> keys() {
        return partitions.stream().map(ParquetPartition::key).toList();
    }

    /**
     * @return All partition values.
     */
    public List<String> values() {
        return partitions.stream().map(ParquetPartition::value).toList();
    }
}
