package io.github.romibuzi.parquetdiff.diff;

import io.github.romibuzi.parquetdiff.metadata.ParquetDetails;
import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaNodePath;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Hold differences between two Parquet schemas.
 */
public final class ParquetSchemaDiff {
    private final ParquetDetails first;
    private final ParquetDetails second;
    private final List<ParquetSchemaNodePath> additionalNodes = new ArrayList<>();
    private final List<ParquetSchemaNodePath> missingNodes = new ArrayList<>();
    private final List<ParquetSchemaTypeDiff> typeDiffs = new ArrayList<>();
    private final List<ParquetSchemaPrimitiveTypeDiff> primitiveTypeDiffs = new ArrayList<>();
    private final List<ParquetSchemaRepetitionDiff> repetitionDiffs = new ArrayList<>();

    /**
     * @param first  The first schema.
     * @param second The second schema.
     */
    ParquetSchemaDiff(ParquetDetails first, ParquetDetails second) {
        this.first = first;
        this.second = second;
    }

    /**
     * Returns whether any differences have been recorded between the two schemas.
     *
     * @return true if any additional, missing, or type differences exist; false otherwise.
     */
    public boolean hasDifferences() {
        return Stream.of(additionalNodes, missingNodes, typeDiffs, primitiveTypeDiffs, repetitionDiffs)
                .anyMatch(list -> !list.isEmpty());
    }

    /**
     * This is the schema that served as reference when establishing differences.
     *
     * @return The first schema.
     */
    public ParquetDetails getFirst() {
        return first;
    }

    /**
     * This is the schema that was compared to the reference when establishing differences.
     *
     * @return The second schema.
     */
    public ParquetDetails getSecond() {
        return second;
    }

    /**
     * @return Additional fields in the second schema.
     */
    public List<ParquetSchemaNodePath> getAdditionalNodes() {
        return Collections.unmodifiableList(additionalNodes);
    }

    /**
     * @return Missing fields in the second schema.
     */
    public List<ParquetSchemaNodePath> getMissingNodes() {
        return Collections.unmodifiableList(missingNodes);
    }

    /**
     * @return Differences fields types (e.g. GROUP vs PRIMITIVE or LIST vs MAP).
     */
    public List<ParquetSchemaTypeDiff> getTypeDiffs() {
        return Collections.unmodifiableList(typeDiffs);
    }

    /**
     * @return Differences fields primitive types (e.g. INT32 vs FLOAT).
     */
    public List<ParquetSchemaPrimitiveTypeDiff> getPrimitiveTypeDiffs() {
        return Collections.unmodifiableList(primitiveTypeDiffs);
    }

    /**
     * @return Differences fields repetition (e.g. OPTIONAL vs REQUIRED).
     */
    public List<ParquetSchemaRepetitionDiff> getRepetitionDiffs() {
        return Collections.unmodifiableList(repetitionDiffs);
    }

    /**
     * Prints a summary of all schema differences.
     *
     * @param out The stream to write into, ex: System.out.
     */
    public void print(PrintStream out) {
        if (!hasDifferences()) {
            out.println("No differences found in " + second.getPartitions() + ".");
            return;
        }
        out.println("Differences found in " + second.getPartitions() + ", compared to " + first.getPartitions() + ":");
        printAdditionalNodes(out);
        printMissingNodes(out);
        printTypeDiffs(out);
        printPrimitiveTypeDiffs(out);
        printRepetitionDiffs(out);
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

    void addRepetitionDiff(ParquetSchemaRepetitionDiff repetitionDiff) {
        repetitionDiffs.add(repetitionDiff);
    }

    void printAdditionalNodes(PrintStream out) {
        additionalNodes.forEach(node -> out.printf("additional field: '%s'.%s", node, System.lineSeparator()));
    }

    void printMissingNodes(PrintStream out) {
        missingNodes.forEach(node -> out.printf("missing field: '%s'.%s", node, System.lineSeparator()));
    }

    void printTypeDiffs(PrintStream out) {
        typeDiffs.forEach(typeDiff -> out.printf("different field type for '%s': '%s' instead of '%s'.%s",
                typeDiff.getNodePath(), typeDiff.getNewType(), typeDiff.getOldType(), System.lineSeparator()));
    }

    void printPrimitiveTypeDiffs(PrintStream out) {
        primitiveTypeDiffs.forEach(primitiveDiff -> out.printf("different field primitive type for '%s': '%s' "
                        + "instead of '%s'.%s", primitiveDiff.getNodePath(), primitiveDiff.getNewType(),
                primitiveDiff.getOldType(), System.lineSeparator()));
    }

    void printRepetitionDiffs(PrintStream out) {
        repetitionDiffs.forEach(repetitionDiff -> out.printf("different repetition for '%s': '%s' "
                        + "instead of '%s'.%s", repetitionDiff.getNodePath(), repetitionDiff.getNewRepetition(),
                repetitionDiff.getOldRepetition(), System.lineSeparator()));
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParquetSchemaDiff that = (ParquetSchemaDiff) o;
        return Objects.equals(first, that.first) && Objects.equals(second, that.second)
                && Objects.equals(additionalNodes, that.additionalNodes)
                && Objects.equals(missingNodes, that.missingNodes)
                && Objects.equals(typeDiffs, that.typeDiffs)
                && Objects.equals(primitiveTypeDiffs, that.primitiveTypeDiffs)
                && Objects.equals(repetitionDiffs, that.repetitionDiffs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second, additionalNodes, missingNodes, typeDiffs, primitiveTypeDiffs,
                repetitionDiffs);
    }
}
