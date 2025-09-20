package io.github.romibuzi.parquetdiff.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ParquetPartitionTest {
    @Test
    void getKeyAndValue() {
        ParquetPartition partition = new ParquetPartition("date", "2022-03-26");
        assertEquals("date", partition.getKey());
        assertEquals("2022-03-26", partition.getValue());
    }
}
