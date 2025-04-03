package com.romibuzi.parquetdiff;

import com.romibuzi.parquetdiff.models.*;

import java.util.*;

public final class ParquetCompare {
    private ParquetCompare() {
    }

    /**
     * Checks if all given {@link ParquetPartitions} have the same partition keys.
     * Any partition with a different set of keys is included in the result.
     *
     * @param parquets Parquets to find differences.
     * @return A list of {@link ParquetPartitions} where the structure differs from the first partition.
     * If differences are found, the first element in the list is the reference partition.
     */
    public static List<ParquetPartitions> findDifferentPartitionsStructure(List<ParquetDetails> parquets) {
        List<ParquetPartitions> partitions = parquets.stream().map(ParquetDetails::partitions).toList();
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
     * Compare all Parquets schemas and find differences.
     *
     * @param parquets Parquets to find schemas differences.
     * @return A list of {@link ParquetSchemaDiff} instances with all differences found.
     */
    public static List<ParquetSchemaDiff> findSchemasDifferences(List<ParquetDetails> parquets) {
        if (parquets.isEmpty()) {
            return Collections.emptyList();
        }

        List<ParquetSchemaDiff> results = new ArrayList<>();

        Iterator<ParquetDetails> iterator = parquets.iterator();
        ParquetDetails reference = iterator.next();

        while (iterator.hasNext()) {
            ParquetDetails parquetDetails = iterator.next();
            ParquetSchemaDiff diff = compareSchemas(reference, parquetDetails);
            if (diff.hasDifferences()) {
                results.add(diff);
                reference = parquetDetails;
            }
        }

        return results;
    }

    /**
     * Compare two ParquetDetails schemas and find differences.
     *
     * @param firstParquet  The first parquet.
     * @param secondParquet The second parquet.
     * @return A {@link ParquetSchemaDiff} instance with all differences found.
     */
    static ParquetSchemaDiff compareSchemas(ParquetDetails firstParquet, ParquetDetails secondParquet) {
        ParquetSchemaDiff diff = new ParquetSchemaDiff(firstParquet, secondParquet);
        compareSchemasNodes(firstParquet.schema(), secondParquet.schema(), "", diff);
        return diff;
    }

    private static void compareSchemasNodes(ParquetSchemaNode node1,
                                            ParquetSchemaNode node2,
                                            String schemaPath,
                                            ParquetSchemaDiff diff) {
        String currentPath = buildSchemaPath(schemaPath, node1.name());

        // TODO handle outside the traversal of nodes as it can only happen at the root level
        if (!node1.name().equals(node2.name())) {
            System.out.println("Name mismatch at " + currentPath + " (" + node1.name() + " vs " + node2.name() + ")");
        }

        findSchemasNodesTypesDifference(node1, node2, currentPath, diff);

        Map<String, ParquetSchemaNode> children1 = node1.getChildrenMap();
        Map<String, ParquetSchemaNode> children2 = node2.getChildrenMap();

        for (String childName : children1.keySet()) {
            if (!children2.containsKey(childName)) {
                diff.addMissingNode(buildSchemaPath(currentPath, childName));
            } else {
                compareSchemasNodes(
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

    private static void findSchemasNodesTypesDifference(ParquetSchemaNode node1,
                                                        ParquetSchemaNode node2,
                                                        String schemaPath,
                                                        ParquetSchemaDiff diff) {
        if (!node1.type().equals(node2.type())) {
            diff.addTypeDiff(new ParquetSchemaTypeDiff(schemaPath, node1.type(), node2.type()));
            return;
        }

        if (ParquetSchemaType.PRIMITIVE == node1.type()) {
            if (!node1.primitiveTypeName().equals(node2.primitiveTypeName())) {
                diff.addPrimitiveTypeDiff(new ParquetSchemaPrimitiveTypeDiff(schemaPath, node1.primitiveTypeName(),
                        node2.primitiveTypeName()));
            }
        }
    }

    private static String buildSchemaPath(String parentPath, String childName) {
        return parentPath.isEmpty() ? childName : parentPath + "." + childName;
    }
}
