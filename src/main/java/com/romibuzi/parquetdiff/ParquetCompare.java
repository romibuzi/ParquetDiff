package com.romibuzi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ParquetCompare {
    public static List<ParquetPartitions> findDifferentPartitionsStructure(List<ParquetDetails> parquetDetails) {
        List<ParquetPartitions> partitions = parquetDetails.stream().map(ParquetDetails::partitions).toList();
        if (partitions.isEmpty()) {
            return Collections.emptyList();
        }

        List<ParquetPartitions> results = new ArrayList<>();

        Iterator<ParquetPartitions> iterator = partitions.iterator();
        ParquetPartitions reference = iterator.next();

        iterator.forEachRemaining(partition -> {
            if (!reference.keys().equals(partition.keys())) {
                results.add(partition);
            }
        });

        if (!results.isEmpty()) {
            results.add(0, reference);
        }

        return results;
    }
}
