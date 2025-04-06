package com.romibuzi.parquetdiff.models;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public record ParquetSchemaDiff(ParquetDetails firstParquet,
                                ParquetDetails secondParquet,
                                List<String> additionalNodes,
                                List<String> missingNodes,
                                List<ParquetSchemaTypeDiff> typeDiffs,
                                List<ParquetSchemaPrimitiveTypeDiff> primitiveTypeDiffs) {
    public ParquetSchemaDiff(ParquetDetails firstParquet, ParquetDetails secondParquet) {
        this(firstParquet, secondParquet, new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    public void addAdditionalNode(String nodePath) {
        additionalNodes.add(nodePath);
    }

    public void addMissingNode(String nodePath) {
        missingNodes.add(nodePath);
    }

    public void addTypeDiff(ParquetSchemaTypeDiff typeDiff) {
        typeDiffs.add(typeDiff);
    }

    public void addPrimitiveTypeDiff(ParquetSchemaPrimitiveTypeDiff primitiveTypeDiff) {
        primitiveTypeDiffs.add(primitiveTypeDiff);
    }

    public boolean hasDifferences() {
        return Stream.of(additionalNodes, missingNodes, typeDiffs, primitiveTypeDiffs)
                .anyMatch(list -> !list.isEmpty());
    }

    public void printDifferences() {
        if (!hasDifferences()) {
            System.out.println("No differences found in: " + secondParquet.partitions());
            return;
        }
        System.out.println("Differences found in: " + secondParquet.partitions());
        printAdditionalNodes();
        printMissingNodes();
        printTypeDiffs();
        printPrimitiveTypeDiffs();
    }

    private void printAdditionalNodes() {
        additionalNodes.forEach(node -> System.out.printf("additional field: '%s'.%n", node));
    }

    private void printMissingNodes() {
        missingNodes.forEach(node -> System.out.printf("missing field: '%s'.%n", node));
    }

    private void printTypeDiffs() {
        typeDiffs.forEach(typeDiff -> System.out.printf("different field type for '%s': '%s' instead of '%s'.%n",
                typeDiff.nodePath(), typeDiff.newType(), typeDiff.oldType()));
    }

    private void printPrimitiveTypeDiffs() {
        primitiveTypeDiffs.forEach(primitiveDiff -> System.out.printf("different field primitive type for '%s': '%s' "
                        + "instead of '%s'.%n", primitiveDiff.nodePath(), primitiveDiff.newType(),
                primitiveDiff.oldType()));
    }
}
