package com.romibuzi.parquetdiff.models;

import org.apache.parquet.schema.PrimitiveType;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a node in a Parquet schema hierarchy.
 * Each node can either be a primitive type or a group with children.
 *
 * <p>
 * Currently, this class only stores {@link PrimitiveType.PrimitiveTypeName} and
 * does not yet handle associated logical types.
 * </p>
 *
 * @param name              The name of the schema node.
 * @param type              The type of the schema node
 * @param primitiveTypeName The primitive type name, applicable if the node is a primitive type.
 * @param children          The list of child nodes if the node is a group type.
 */
public record ParquetSchemaNode(String name,
                                ParquetSchemaType type,
                                PrimitiveType.PrimitiveTypeName primitiveTypeName,
                                List<ParquetSchemaNode> children) {
    public ParquetSchemaNode(String name,
                             ParquetSchemaType type,
                             PrimitiveType.PrimitiveTypeName primitiveTypeName) {
        this(name, type, primitiveTypeName, new ArrayList<>());
    }

    /**
     * Adds a child node to this schema node.
     *
     * @param child The child node to be added.
     */
    public void addChild(ParquetSchemaNode child) {
        children.add(child);
    }

    /**
     * @return true if node have children
     */
    public boolean hasChildren() {
        return !children.isEmpty();
    }
}
