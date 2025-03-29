package com.romibuzi.parquetdiff.models;

import org.apache.parquet.schema.PrimitiveType;

import java.util.ArrayList;
import java.util.List;

// TODO, only stores PrimitiveTypeName for now and not yet associated LogicalType
public record ParquetSchemaNode(String name,
                                ParquetSchemaType type,
                                PrimitiveType.PrimitiveTypeName primitiveTypeName,
                                List<ParquetSchemaNode> children) {
    public ParquetSchemaNode(String name, ParquetSchemaType type, PrimitiveType.PrimitiveTypeName primitiveTypeName) {
        this(name, type, primitiveTypeName, new ArrayList<>());
    }

    public void addChild(ParquetSchemaNode child) {
        children.add(child);
    }
}
