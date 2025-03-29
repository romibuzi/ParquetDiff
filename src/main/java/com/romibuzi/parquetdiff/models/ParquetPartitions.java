package com.romibuzi.parquetdiff.models;

import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.List;

public record ParquetPartitions(List<ParquetPartition> partitions) {
    public static ParquetPartitions fromPath(Path path) {
        List<ParquetPartition> partitions =
                Arrays.stream(path.toUri().getPath().split("/"))
                        .filter(part -> part.contains("="))
                        .map(ParquetPartition::fromString)
                        .toList();

        return new ParquetPartitions(partitions);
    }

    public List<String> keys() {
        return partitions.stream().map(ParquetPartition::key).toList();
    }

    public List<String> values() {
        return partitions.stream().map(ParquetPartition::value).toList();
    }
}
