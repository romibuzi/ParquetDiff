package com.romibuzi.parquetdiff.models;

import org.apache.parquet.schema.PrimitiveType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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
     * Adds a child node to this schema/group node.
     *
     * @param child The child node to be added.
     */
    public void addChild(ParquetSchemaNode child) {
        if (ParquetSchemaType.PRIMITIVE == type) {
            throw new UnsupportedOperationException("PRIMITIVE field can't have child field");
        }

        children.add(child);
    }

    /**
     * @return A Map representation of node's children.
     */
    public Map<String, ParquetSchemaNode> getChildrenMap() {
        return children.stream().collect(Collectors.toMap(ParquetSchemaNode::name, Function.identity()));
    }

    /**
     * @return true if node have children.
     */
    public boolean hasChildren() {
        return !children.isEmpty();
    }

    /**
     * @param other the other node.
     * @return true if the other node has the same type as the current one.
     */
    public boolean hasSameType(ParquetSchemaNode other) {
        return type.equals(other.type);
    }

    /**
     * @param other the other node.
     * @return true if the other node has the same primitive type as the current one.
     */
    public boolean hasSamePrimitiveType(ParquetSchemaNode other) {
        if (ParquetSchemaType.PRIMITIVE != type) {
            return true; // Don't run the comparison if node is not a primitive
        }
        return primitiveTypeName.equals(other.primitiveTypeName);
    }

    /**
     * Prints the Schema from the Root node in a tree format.
     */
    public void printSchema() {
        if (ParquetSchemaType.MESSAGE != type) {
            throw new UnsupportedOperationException("printSchema() can only be applied to the Root node");
        }
        printNode(this, 0);
    }

    private void printNode(ParquetSchemaNode node, int indent) {
        String prefix = indent > 0 ? " ".repeat(indent) + "|-- " : "";

        String infos = switch (node.type()) {
            case MESSAGE, GROUP, LIST, MAP -> node.type().toString();
            case PRIMITIVE -> node.primitiveTypeName().name().toLowerCase(Locale.ROOT);
        };

        System.out.println(prefix + node.name() + ": " + infos);

        for (ParquetSchemaNode child : node.children()) {
            printNode(child, indent + 2);
        }
    }
}
