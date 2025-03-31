package com.romibuzi.parquetdiff;

import com.romibuzi.parquetdiff.models.ParquetDetails;
import com.romibuzi.parquetdiff.models.ParquetPartitions;
import com.romibuzi.parquetdiff.models.ParquetSchemaNode;
import com.romibuzi.parquetdiff.models.ParquetSchemaType;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.PrimitiveType;
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
        String path = "vaccinations.parquet/date=2020-12-28/country=Spain/part-0000.parquet";
        ParquetDetails first = new ParquetDetails(new Path(path), 10, null);
        List<ParquetPartitions> results = ParquetCompare.findDifferentPartitionsStructure(List.of(first));
        assertTrue(results.isEmpty());
    }

    @Test
    void findDifferentPartitionsStructureAllSame() {
        String firstPath = "vaccinations.parquet/date=2020-12-28/country=Spain/part-0000.parquet";
        String secondPath = "vaccinations.parquet/date=2020-12-29/country=Spain/part-0000.parquet";
        ParquetDetails first = new ParquetDetails(new Path(firstPath), 10, null);
        ParquetDetails second = new ParquetDetails(new Path(secondPath), 10, null);
        List<ParquetPartitions> results = ParquetCompare.findDifferentPartitionsStructure(List.of(first, second));
        assertTrue(results.isEmpty());
    }

    @Test
    void findDifferentPartitionsStructure() {
        String firstPath = "vaccinations.parquet/date=2020-12-28/country=Spain/part-0000.parquet";
        String secondPath = "vaccinations.parquet/date=2020-12-29/part-0000.parquet";
        ParquetDetails first = new ParquetDetails(new Path(firstPath), 10, null);
        ParquetDetails second = new ParquetDetails(new Path(secondPath), 10, null);
        List<ParquetPartitions> results = ParquetCompare.findDifferentPartitionsStructure(List.of(first, second));
        assertEquals(2, results.size());
        assertEquals(List.of("date", "country"), results.get(0).keys());
        assertEquals(List.of("date"), results.get(1).keys());
    }
}
