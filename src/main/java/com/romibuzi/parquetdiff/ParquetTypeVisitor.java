package com.romibuzi.parquetdiff;

import com.romibuzi.parquetdiff.metadata.ParquetSchemaNode;
import com.romibuzi.parquetdiff.metadata.ParquetSchemaType;
import org.apache.parquet.schema.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Stack;

/**
 * A TypeVisitor implementation to extract a given {@link MessageType} into a {@link ParquetSchemaNode}.
 */
final class ParquetTypeVisitor implements TypeVisitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetTypeVisitor.class);
    private ParquetSchemaNode root;
    private final Stack<ParquetSchemaNode> nodeStack = new Stack<>();

    @Override
    public void visit(MessageType messageType) {
        root = new ParquetSchemaNode(ParquetSchemaType.MESSAGE.toString(), ParquetSchemaType.MESSAGE, null, null);
        enterNewParent(root);
        for (Type field : messageType.getFields()) {
            field.accept(this);
        }
        exitCurrentParent();
    }

    @Override
    public void visit(GroupType groupType) {
        ParquetSchemaNode node = new ParquetSchemaNode(groupType.getName(), findGroupType(groupType), null, null);
        attachToCurrentParent(node);
        enterNewParent(node);
        for (Type field : groupType.getFields()) {
            field.accept(this);
        }
        exitCurrentParent();
    }

    @Override
    public void visit(PrimitiveType primitiveType) {
        ParquetSchemaNode node = new ParquetSchemaNode(primitiveType.getName(), ParquetSchemaType.PRIMITIVE,
                primitiveType.getPrimitiveTypeName(), primitiveType.getLogicalTypeAnnotation());
        attachToCurrentParent(node);
    }

    /**
     * @return The extracted {@link ParquetSchemaNode} schema representation.
     */
    public ParquetSchemaNode getSchema() {
        return root;
    }

    ParquetSchemaType findGroupType(GroupType groupType) {
        LogicalTypeAnnotation logicalType = groupType.getLogicalTypeAnnotation();
        if (logicalType == null) {
            return ParquetSchemaType.GROUP;
        } else if (logicalType == LogicalTypeAnnotation.listType()) {
            return ParquetSchemaType.LIST;
        } else if (logicalType == LogicalTypeAnnotation.mapType()) {
            return ParquetSchemaType.MAP;
        }

        LOGGER.warn("Unrecognized Group logical type: {}", logicalType);
        return ParquetSchemaType.GROUP;
    }

    private void attachToCurrentParent(ParquetSchemaNode node) {
        getCurrentParent().addChild(node);
    }

    private void enterNewParent(ParquetSchemaNode node) {
        nodeStack.push(node);
    }

    private void exitCurrentParent() {
        nodeStack.pop();
    }

    private ParquetSchemaNode getCurrentParent() {
        return nodeStack.peek();
    }
}
