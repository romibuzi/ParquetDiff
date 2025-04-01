package com.romibuzi.parquetdiff.models;

import java.util.ArrayList;
import java.util.List;

public record ParquetSchemaDiff(List<String> additionalNodes, List<String> missingNodes) {
    public ParquetSchemaDiff() {
        this(new ArrayList<>(), new ArrayList<>());
    }

    public void addAdditionalNode(String nodePath) {
        this.additionalNodes.add(nodePath);
    }

    public void addMissingNode(String nodePath) {
        this.missingNodes.add(nodePath);
    }

    public boolean hasDifferences() {
        return !additionalNodes.isEmpty() || !missingNodes.isEmpty();
    }
}
