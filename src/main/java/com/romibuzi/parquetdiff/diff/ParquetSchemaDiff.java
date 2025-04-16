package com.romibuzi.parquetdiff.diff;

import com.romibuzi.parquetdiff.metadata.ParquetDetails;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Hold differences between two Parquet schemas.
 *
 * @param first              The first schema.
 * @param second             The second schema.
 * @param additionalNodes    Additional fields in the second schema.
 * @param missingNodes       Missing fields in the second schema.
 * @param typeDiffs          Differences fields types (e.g. GROUP vs PRIMITIVE or LIST vs MAP)
 * @param primitiveTypeDiffs Differences fields primitive types (e.g. INT32 vs FLOAT)
 */
public record ParquetSchemaDiff(ParquetDetails first,
                                ParquetDetails second,
                                List<String> additionalNodes,
                                List<String> missingNodes,
                                List<ParquetSchemaTypeDiff> typeDiffs,
                                List<ParquetSchemaPrimitiveTypeDiff> primitiveTypeDiffs) {
    ParquetSchemaDiff(ParquetDetails first, ParquetDetails second) {
        this(first, second, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    /**
     * Returns whether any differences have been recorded between the two schemas.
     *
     * @return true if any additional, missing, or type differences exist; false otherwise.
     */
    public boolean hasDifferences() {
        return Stream.of(additionalNodes, missingNodes, typeDiffs, primitiveTypeDiffs)
                .anyMatch(list -> !list.isEmpty());
    }

    /**
     * Prints a summary of all schema differences.
     */
    public void printDifferences() {
        if (!hasDifferences()) {
            System.out.println("No differences found in " + second.partitions() + ".");
            return;
        }
        System.out.println("Differences found in " + second.partitions() + ", compared to " + first.partitions() + ":");
        printAdditionalNodes();
        printMissingNodes();
        printTypeDiffs();
        printPrimitiveTypeDiffs();
    }

    void addAdditionalNode(String nodePath) {
        additionalNodes.add(nodePath);
    }

    void addMissingNode(String nodePath) {
        missingNodes.add(nodePath);
    }

    void addTypeDiff(ParquetSchemaTypeDiff typeDiff) {
        typeDiffs.add(typeDiff);
    }

    void addPrimitiveTypeDiff(ParquetSchemaPrimitiveTypeDiff primitiveTypeDiff) {
        primitiveTypeDiffs.add(primitiveTypeDiff);
    }

    private void printAdditionalNodes() {
        additionalNodes.forEach(node -> System.out.printf("additional field: '%s'.%s", node, System.lineSeparator()));
    }

    private void printMissingNodes() {
        missingNodes.forEach(node -> System.out.printf("missing field: '%s'.%s", node, System.lineSeparator()));
    }

    private void printTypeDiffs() {
        typeDiffs.forEach(typeDiff -> System.out.printf("different field type for '%s': '%s' instead of '%s'.%s",
                typeDiff.nodePath(), typeDiff.newType(), typeDiff.oldType(), System.lineSeparator()));
    }

    private void printPrimitiveTypeDiffs() {
        primitiveTypeDiffs.forEach(primitiveDiff -> System.out.printf("different field primitive type for '%s': '%s' "
                        + "instead of '%s'.%s", primitiveDiff.nodePath(), primitiveDiff.newType(),
                primitiveDiff.oldType(), System.lineSeparator()));
    }
}
