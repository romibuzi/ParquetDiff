package io.github.romibuzi.parquetdiff.metadata;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.io.PrintStream;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents either a complete Parquet schema or a node in a schema.
 * Each node can either be a primitive type or a group with children.
 */
public final class ParquetSchemaNode {
    private final String name;
    private final ParquetSchemaType type;
    private final PrimitiveType.PrimitiveTypeName primitiveType;
    private final LogicalTypeAnnotation logicalType;
    private final List<ParquetSchemaNode> children;

    /**
     * @param name          The name of the schema node.
     * @param type          The type of the schema node
     * @param primitiveType The primitive type name, applicable if the node is a primitive.
     * @param logicalType   Optional, logical type associated to the primitive, applicable if the node is a primitive.
     * @param children      The list of child nodes if the node is a group type.
     */
    public ParquetSchemaNode(String name, ParquetSchemaType type, PrimitiveType.PrimitiveTypeName primitiveType,
                             LogicalTypeAnnotation logicalType, List<ParquetSchemaNode> children) {
        this.name = name;
        this.type = type;
        this.primitiveType = primitiveType;
        this.logicalType = logicalType;
        this.children = children;
    }

    /**
     * Creates a ParquetSchemaNode instance without children.
     */
    public ParquetSchemaNode(String name, ParquetSchemaType type, PrimitiveType.PrimitiveTypeName primitiveType,
                             LogicalTypeAnnotation logicalType) {
        this(name, type, primitiveType, logicalType, new ArrayList<>());
    }

    /**
     * Adds a child node to this schema/group node.
     *
     * @param child The child node to be added.
     * @throws UnsupportedOperationException if the node is not a group node.
     */
    public void addChild(ParquetSchemaNode child) {
        if (ParquetSchemaType.PRIMITIVE == type) {
            throw new UnsupportedOperationException("Primitive field can't have children");
        }

        children.add(child);
    }

    public String getName() {
        return name;
    }

    public ParquetSchemaType getType() {
        return type;
    }

    public PrimitiveType.PrimitiveTypeName getPrimitiveType() {
        return primitiveType;
    }

    public LogicalTypeAnnotation getLogicalType() {
        return logicalType;
    }

    public List<ParquetSchemaNode> getChildren() {
        return children;
    }

    /**
     * @return A Map representation of node's children.
     */
    public Map<String, ParquetSchemaNode> getChildrenMap() {
        return children.stream().collect(Collectors.toMap(ParquetSchemaNode::getName, Function.identity()));
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
        return primitiveType.equals(other.primitiveType) && Objects.equals(logicalType, other.logicalType);
    }

    /**
     * Prints the Schema from the Root node in a tree format.
     */
    public void print(PrintStream printStream) {
        if (ParquetSchemaType.MESSAGE != type) {
            throw new UnsupportedOperationException("print() can only be applied to the Root node");
        }
        printNode(this, printStream, 0);
    }

    private void printNode(ParquetSchemaNode node, PrintStream printStream, int indent) {
        String prefix = indent > 0 ? " ".repeat(indent) + "|-- " : "";

        String infos;
        switch (node.type) {
            case GROUP:
            case LIST:
            case MAP:
                infos = node.type.toString();
                break;
            case PRIMITIVE:
                infos = node.primitiveName();
                break;
            case MESSAGE:
            default:
                infos = "";
                break;
        }

        printStream.println(prefix + node.name + ": " + infos);

        for (ParquetSchemaNode child : node.children) {
            printNode(child, printStream, indent + 2);
        }
    }

    private String primitiveName() {
        if (ParquetSchemaType.PRIMITIVE != type) {
            throw new UnsupportedOperationException("primitiveName() can only be applied to a Primitive node");
        }
        String primitiveTypeLower = primitiveType.name().toLowerCase(Locale.ROOT);
        if (logicalType != null) {
            return logicalType.toString().toLowerCase(Locale.ROOT) + " (" + primitiveTypeLower + ")";
        }
        return primitiveTypeLower;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParquetSchemaNode that = (ParquetSchemaNode) o;
        return Objects.equals(name, that.name) && type == that.type && primitiveType == that.primitiveType
                && Objects.equals(logicalType, that.logicalType) && Objects.equals(children, that.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, primitiveType, logicalType, children);
    }
}
