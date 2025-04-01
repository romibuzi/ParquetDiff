package com.romibuzi.parquetdiff;

import com.romibuzi.parquetdiff.models.ParquetDetails;
import com.romibuzi.parquetdiff.models.ParquetPartitions;
import com.romibuzi.parquetdiff.models.ParquetSchemaDiff;
import com.romibuzi.parquetdiff.models.ParquetSchemaNode;

import java.util.*;

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
     * @return Differences found.
     */
    public static ParquetSchemaDiff findSchemasDifferences(ParquetSchemaNode schema1, ParquetSchemaNode schema2) {
        ParquetSchemaDiff diff = new ParquetSchemaDiff();
        findSchemasDifferencesRecursive(schema1, schema2, "", diff);
        return diff;
    }

    private static void findSchemasDifferencesRecursive(ParquetSchemaNode node1,
                                                        ParquetSchemaNode node2,
                                                        String schemaPath,
                                                        ParquetSchemaDiff diff) {
        String currentPath = buildSchemaPath(schemaPath, node1.name());

        if (!node1.name().equals(node2.name())) {
            System.out.println("Name mismatch at " + currentPath + " (" + node1.name() + " vs " + node2.name() + ")");
        }
        if (!node1.type().equals(node2.type())) {
            System.out.println("Type mismatch at " + currentPath + " (" + node1.type() + " vs " + node2.type() + ")");
        }

        Map<String, ParquetSchemaNode> children1 = node1.getChildrenMap();
        Map<String, ParquetSchemaNode> children2 = node2.getChildrenMap();

        for (String childName : children1.keySet()) {
            if (!children2.containsKey(childName)) {
                diff.addMissingNode(buildSchemaPath(currentPath, childName));
            } else {
                findSchemasDifferencesRecursive(
                        children1.get(childName),
                        children2.get(childName),
                        currentPath,
                        diff);
            }
        }

        for (String childName : children2.keySet()) {
            if (!children1.containsKey(childName)) {
                diff.addAdditionalNode(buildSchemaPath(currentPath, childName));
            }
        }
    }

    private static String buildSchemaPath(String parentPath, String childName) {
        return parentPath.isEmpty() ? childName : parentPath + "." + childName;
    }
}
