package io.github.romibuzi.parquetdiff.metadata;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParquetPartitionsTest {
    @Test
    void fromPath() {
        Path path = new Path("hdfs:/data/archive.parquet/date=2022-03-26/customer=1234/part-000.parquet");
        ParquetPartitions result = ParquetPartitions.fromPath(path);

        assertEquals(2, result.getPartitions().size());
        assertEquals(new ParquetPartition("date", "2022-03-26"), result.getPartitions().get(0));
        assertEquals(new ParquetPartition("customer", "1234"), result.getPartitions().get(1));
    }

    @Test
    void fromPathEmptyPartition() {
        Path path = new Path("hdfs:/data/archive.parquet/date=/part-000.parquet");
        ParquetPartitions result = ParquetPartitions.fromPath(path);

        assertEquals(1, result.getPartitions().size());
        assertEquals(new ParquetPartition("date", ""), result.getPartitions().get(0));
    }

    @Test
    void fromPathNoPartition() {
        Path path = new Path("hdfs:/data/archive.parquet/part-000.parquet");
        ParquetPartitions result = ParquetPartitions.fromPath(path);

        assertTrue(result.getPartitions().isEmpty());
    }
}
