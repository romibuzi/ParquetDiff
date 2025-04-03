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
}
