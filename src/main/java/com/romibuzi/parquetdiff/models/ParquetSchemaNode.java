package com.romibuzi.parquetdiff.models;

import org.apache.parquet.schema.PrimitiveType;

import java.util.ArrayList;
import java.util.List;

public record ParquetSchemaNode(String name, ParquetSchemaType type, PrimitiveType primitiveType,
                                List<ParquetSchemaNode> children) {
    public ParquetSchemaNode(String name, ParquetSchemaType type, PrimitiveType primitiveType) {
        this(name, type, primitiveType, new ArrayList<>());
    }

    public void addChild(ParquetSchemaNode child) {
        children.add(child);
    }
}
