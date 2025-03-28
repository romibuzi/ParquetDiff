package com.romibuzi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: ParquetDiff <parquet-path>");
            System.exit(1);
        }
        String parquetPath = args[0];

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);

        ParquetReader parquetReader = new ParquetReader(fileSystem);
        List<ParquetDetails> parquets = parquetReader.readParquetDirectory(parquetPath);

        List<ParquetPartitions> differentPartitionsStructure = ParquetCompare.findDifferentPartitionsStructure(parquets);

        if (differentPartitionsStructure.isEmpty()) {
            System.out.println("All Parquet partitions have the same structure");
        }
    }
}
