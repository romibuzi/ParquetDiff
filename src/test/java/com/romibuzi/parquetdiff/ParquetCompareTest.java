package com.romibuzi.parquetdiff;

import com.romibuzi.parquetdiff.models.ParquetDetails;
import com.romibuzi.parquetdiff.models.ParquetPartitions;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParquetCompareTest {
    @Test
    void findDifferentPartitionsStructureEmpty() {
        List<ParquetPartitions> results = ParquetCompare.findDifferentPartitionsStructure(Collections.emptyList());
        assertTrue(results.isEmpty());
    }

    @Test
    void findDifferentPartitionsStructureSinglePartition() {
        ParquetDetails first = new ParquetDetails(new Path("vaccinations.parquet/date=2020-12-28/country=Spain/part-0000.parquet"), 10, null);
        List<ParquetPartitions> results = ParquetCompare.findDifferentPartitionsStructure(List.of(first));
        assertTrue(results.isEmpty());
    }

    @Test
    void findDifferentPartitionsStructureAllSame() {
        ParquetDetails first = new ParquetDetails(new Path("vaccinations.parquet/date=2020-12-28/country=Spain/part-0000.parquet"), 10, null);
        ParquetDetails second = new ParquetDetails(new Path("vaccinations.parquet/date=2020-12-29/country=Spain/part-0000.parquet"), 10, null);
        List<ParquetPartitions> results = ParquetCompare.findDifferentPartitionsStructure(List.of(first, second));
        assertTrue(results.isEmpty());
    }

    @Test
    void findDifferentPartitionsStructure() {
        ParquetDetails first = new ParquetDetails(new Path("vaccinations.parquet/date=2020-12-28/country=Spain/part-0000.parquet"), 10, null);
        ParquetDetails second = new ParquetDetails(new Path("vaccinations.parquet/date=2020-12-29/part-0000.parquet"), 10, null);
        List<ParquetPartitions> results = ParquetCompare.findDifferentPartitionsStructure(List.of(first, second));
        assertEquals(2, results.size());
        assertEquals(List.of("date", "country"), results.get(0).keys());
        assertEquals(List.of("date"), results.get(1).keys());
    }
}
