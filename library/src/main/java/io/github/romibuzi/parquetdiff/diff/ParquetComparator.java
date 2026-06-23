package io.github.romibuzi.parquetdiff.diff;

import io.github.romibuzi.parquetdiff.metadata.ParquetDetails;
import io.github.romibuzi.parquetdiff.metadata.ParquetPartitions;
import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaNode;
import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaNodePath;

import java.util.*;

/**
 * Provide facilities to compare Parquet Schemas.
 */
public interface ParquetComparator {

    /**
     * Checks if all given {@link ParquetPartitions} have the same partition keys.
     * Any partition with a different set of keys is included in the result.
     *
     * @param parquets Parquets to find differences.
     * @return A list of {@link ParquetPartitions} where the structure differs from the first partition.
     * If differences are found, the first element in the list is the reference partition.
     */
    static List<ParquetPartitions> findDifferentPartitionsStructure(List<ParquetDetails> parquets) {
        if (parquets.isEmpty()) {
            return Collections.emptyList();
        }

        List<ParquetPartitions> results = new ArrayList<>();

        Iterator<ParquetDetails> iterator = parquets.iterator();
        ParquetPartitions reference = iterator.next().getPartitions();

        iterator.forEachRemaining(parquet -> {
            ParquetPartitions partition = parquet.getPartitions();
            if (!reference.getKeys().equals(partition.getKeys())) {
                results.add(partition);
            }
        });

        if (!results.isEmpty()) {
            results.add(0, reference);
        }

        return results;
    }

    /**
     * Iterates over a list of Parquet schemas and compares them by pair to detect differences.
     * The first schema in the list serves as the initial reference. If differences are detected during a comparison,
     * the reference becomes the current one in the iteration.
     * This strategy helps to limit the number of differences reported when schema evolution occurs over time in a
     * Parquet directory.
     * <p>
     * For example, given three Parquet files with different partitions values: A, B, and C,
     * the comparison starts with A and B (using A as the reference). If differences are found, they are returned and
     * B becomes the new reference for the next comparison. The process then continues with B and C, and so on.
     *
     * @param parquets Parquets to find schemas differences.
     * @return A list of {@link ParquetSchemaDiff} instances with all differences found.
     */
    static List<ParquetSchemaDiff> findSchemasDifferences(List<ParquetDetails> parquets) {
        if (parquets.isEmpty()) {
            return Collections.emptyList();
        }

        List<ParquetSchemaDiff> results = new ArrayList<>();

        Iterator<ParquetDetails> iterator = parquets.iterator();
        ParquetDetails reference = iterator.next();

        while (iterator.hasNext()) {
            ParquetDetails parquet = iterator.next();
            ParquetSchemaDiff diff = compareSchemas(reference, parquet);
            if (diff.hasDifferences()) {
                results.add(diff);
                reference = parquet;
            }
        }

        return results;
    }

    /**
     * @see ParquetComparator#findSchemasDifferences(List)
     */
    static Optional<ParquetSchemaDiff> findSchemasDifferences(ParquetDetails first, ParquetDetails second) {
        List<ParquetSchemaDiff> differences = findSchemasDifferences(List.of(first, second));
        if (differences.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(differences.get(0));
    }

    /**
     * Compare two ParquetDetails schemas and find differences.
     *
     * @param firstParquet  The first Parquet.
     * @param secondParquet The second Parquet.
     * @return A {@link ParquetSchemaDiff} instance with all differences found.
     */
    static ParquetSchemaDiff compareSchemas(ParquetDetails firstParquet, ParquetDetails secondParquet) {
        ParquetSchemaDiff diff = new ParquetSchemaDiff(firstParquet, secondParquet);
        compareSchemasNodes(firstParquet.getSchema(), secondParquet.getSchema(), new ParquetSchemaNodePath(), diff);
        return diff;
    }

    private static void compareSchemasNodes(ParquetSchemaNode first,
                                            ParquetSchemaNode second,
                                            ParquetSchemaNodePath path,
                                            ParquetSchemaDiff diff) {
        ParquetSchemaNodePath currentPath = path.add(first.getName());

        findSchemasNodesDifferences(first, second, currentPath, diff);

        Map<String, ParquetSchemaNode> firstChildren = first.getChildrenMap();
        Map<String, ParquetSchemaNode> secondChildren = second.getChildrenMap();

        for (Map.Entry<String, ParquetSchemaNode> firstChild : firstChildren.entrySet()) {
            ParquetSchemaNode secondChild = secondChildren.get(firstChild.getKey());
            if (secondChild == null) {
                diff.addMissingNode(currentPath.add(firstChild.getKey()));
            } else {
                compareSchemasNodes(firstChild.getValue(), secondChild, currentPath, diff);
            }
        }

        for (String childName : secondChildren.keySet()) {
            if (!firstChildren.containsKey(childName)) {
                diff.addAdditionalNode(currentPath.add(childName));
            }
        }
    }

    private static void findSchemasNodesDifferences(ParquetSchemaNode first, ParquetSchemaNode second,
                                                    ParquetSchemaNodePath path, ParquetSchemaDiff diff) {
        if (!first.hasSameRepetition(second)) {
            diff.addRepetitionDiff(new ParquetSchemaRepetitionDiff(path, first.getRepetition(),
                    second.getRepetition()));
        }

        if (!first.hasSameType(second)) {
            diff.addTypeDiff(new ParquetSchemaTypeDiff(path, first.getType(), second.getType()));
            return; // no need to further compare primitive types if one node is not primitive
        }

        if (!first.hasSamePrimitiveType(second)) {
            diff.addPrimitiveTypeDiff(new ParquetSchemaPrimitiveTypeDiff(path, first.getPrimitiveType(),
                    second.getPrimitiveType()));
        }
    }
}
