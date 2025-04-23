package io.github.romibuzi.parquetdiff.diff;

import io.github.romibuzi.parquetdiff.metadata.ParquetDetails;
import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaNodePath;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Hold differences between two Parquet schemas.
 */
public final class ParquetSchemaDiff {
    private final ParquetDetails first;
    private final ParquetDetails second;
    private final List<ParquetSchemaNodePath> additionalNodes;
    private final List<ParquetSchemaNodePath> missingNodes;
    private final List<ParquetSchemaTypeDiff> typeDiffs;
    private final List<ParquetSchemaPrimitiveTypeDiff> primitiveTypeDiffs;

    /**
     * @param first  The first schema.
     * @param second The second schema.
     */
    public ParquetSchemaDiff(ParquetDetails first, ParquetDetails second) {
        this.first = first;
        this.second = second;
        this.additionalNodes = new ArrayList<>();
        this.missingNodes = new ArrayList<>();
        this.typeDiffs = new ArrayList<>();
        this.primitiveTypeDiffs = new ArrayList<>();
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
     * @return Additional fields in the second schema.
     */
    public List<ParquetSchemaNodePath> getAdditionalNodes() {
        return additionalNodes;
    }

    /**
     * @return Missing fields in the second schema.
     */
    public List<ParquetSchemaNodePath> getMissingNodes() {
        return missingNodes;
    }

    /**
     * @return Differences fields types (e.g. GROUP vs PRIMITIVE or LIST vs MAP).
     */
    public List<ParquetSchemaTypeDiff> getTypeDiffs() {
        return typeDiffs;
    }

    /**
     * @return Differences fields primitive types (e.g. INT32 vs FLOAT).
     */
    public List<ParquetSchemaPrimitiveTypeDiff> getPrimitiveTypeDiffs() {
        return primitiveTypeDiffs;
    }

    /**
     * Prints a summary of all schema differences.
     *
     * @param printStream The stream to write into, ex: System.out.
     */
    public void printDifferences(PrintStream printStream) {
        if (!hasDifferences()) {
            printStream.println("No differences found in " + second.getPartitions() + ".");
            return;
        }
        printStream.println("Differences found in " + second.getPartitions() + ", compared to " + first.getPartitions()
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

    void printAdditionalNodes(PrintStream printStream) {
        additionalNodes.forEach(node -> printStream.printf("additional field: '%s'.%s", node, System.lineSeparator()));
    }

    void printMissingNodes(PrintStream printStream) {
        missingNodes.forEach(node -> printStream.printf("missing field: '%s'.%s", node, System.lineSeparator()));
    }

    void printTypeDiffs(PrintStream printStream) {
        typeDiffs.forEach(typeDiff -> printStream.printf("different field type for '%s': '%s' instead of '%s'.%s",
                typeDiff.getNodePath(), typeDiff.getNewType(), typeDiff.getOldType(), System.lineSeparator()));
    }

    void printPrimitiveTypeDiffs(PrintStream printStream) {
        primitiveTypeDiffs.forEach(primitiveDiff -> printStream.printf("different field primitive type for '%s': '%s' "
                        + "instead of '%s'.%s", primitiveDiff.getNodePath(), primitiveDiff.getNewType(),
                primitiveDiff.getOldType(), System.lineSeparator()));
    }
}
