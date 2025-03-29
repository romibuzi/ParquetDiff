package com.romibuzi;

import org.apache.hadoop.fs.Path;

public record ParquetDetails(Path path, long numRows, ParquetPartitions partitions) {
    public ParquetDetails(Path path, long numRows) {
        this(path, numRows, ParquetPartitions.fromPath(path));
    }
}
