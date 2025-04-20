package io.github.romibuzi.parquetdiff.diff;

import io.github.romibuzi.parquetdiff.metadata.ParquetDetails;
import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaNodePath;

import java.io.PrintStream;
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
                                List<ParquetSchemaNodePath> additionalNodes,
                                List<ParquetSchemaNodePath> missingNodes,
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
     *
     * @param printStream the stream to write into.
     */
    public void printDifferences(PrintStream printStream) {
        if (!hasDifferences()) {
            printStream.println("No differences found in " + second.partitions() + ".");
            return;
        }
        printStream.println("Differences found in " + second.partitions() + ", compared to " + first.partitions()
                + ":");
        printAdditionalNodes(printStream);
        printMissingNodes(printStream);
        printTypeDiffs(printStream);
        printPrimitiveTypeDiffs(printStream);
    }

    void addAdditionalNode(ParquetSchemaNodePath nodePath) {
        additionalNodes.add(nodePath);
    }

    void addMissingNode(ParquetSchemaNodePath nodePath) {
        missingNodes.add(nodePath);
    }

    void addTypeDiff(ParquetSchemaTypeDiff typeDiff) {
        typeDiffs.add(typeDiff);
    }

    void addPrimitiveTypeDiff(ParquetSchemaPrimitiveTypeDiff primitiveTypeDiff) {
        primitiveTypeDiffs.add(primitiveTypeDiff);
    }

    private void printAdditionalNodes(PrintStream printStream) {
        additionalNodes.forEach(node -> printStream.printf("additional field: '%s'.%s", node, System.lineSeparator()));
    }

    private void printMissingNodes(PrintStream printStream) {
        missingNodes.forEach(node -> printStream.printf("missing field: '%s'.%s", node, System.lineSeparator()));
    }

    private void printTypeDiffs(PrintStream printStream) {
        typeDiffs.forEach(typeDiff -> printStream.printf("different field type for '%s': '%s' instead of '%s'.%s",
                typeDiff.nodePath(), typeDiff.newType(), typeDiff.oldType(), System.lineSeparator()));
    }

    private void printPrimitiveTypeDiffs(PrintStream printStream) {
        primitiveTypeDiffs.forEach(primitiveDiff -> printStream.printf("different field primitive type for '%s': '%s' "
                        + "instead of '%s'.%s", primitiveDiff.nodePath(), primitiveDiff.newType(),
                primitiveDiff.oldType(), System.lineSeparator()));
    }
}
