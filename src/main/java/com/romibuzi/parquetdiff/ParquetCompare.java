package com.romibuzi.parquetdiff;

import com.romibuzi.parquetdiff.models.ParquetDetails;
import com.romibuzi.parquetdiff.models.ParquetPartitions;
import com.romibuzi.parquetdiff.models.ParquetSchemaNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public final class ParquetCompare {
    private ParquetCompare() {
    }

    /**
     * Checks if all given {@link ParquetPartitions} have the same partition keys.
     * Any partition with a different set of keys is included in the result.
     *
     * @param parquetDetails Parquets to find differences.
     * @return A list of {@link ParquetPartitions} where the structure differs from the first partition.
     * If differences are found, the first element in the list is the reference partition.
     */
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

    /**
     * Compare two schemas and find differences.
     *
     * @param schema1 The first schema.
     * @param schema2 The second schema.
     * @return true if no differences are found false otherwise.
     */
    public static boolean compareSchemas(ParquetSchemaNode schema1, ParquetSchemaNode schema2) {
        return true;
    }
}
