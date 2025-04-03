package com.romibuzi.parquetdiff;

import com.romibuzi.parquetdiff.models.ParquetSchemaNode;
import com.romibuzi.parquetdiff.models.ParquetSchemaType;
import org.apache.parquet.schema.*;

import java.util.Stack;

/**
 * A TypeVisitor implementation to extract a given {@link MessageType} into a {@link ParquetSchemaNode}.
 */
public final class ParquetTypeVisitor implements TypeVisitor {
    private ParquetSchemaNode root;
    private final Stack<ParquetSchemaNode> nodeStack = new Stack<>();

    @Override
    public void visit(MessageType messageType) {
        root = new ParquetSchemaNode(messageType.getName(), ParquetSchemaType.MESSAGE, null);
        enterNewParent(root);
        for (Type field : messageType.getFields()) {
            field.accept(this);
        }
        exitCurrentParent();
    }

    @Override
    public void visit(GroupType groupType) {
        ParquetSchemaNode node = new ParquetSchemaNode(groupType.getName(), ParquetSchemaType.GROUP, null);
        attachToCurrentParent(node);
        enterNewParent(node);
        for (Type field : groupType.getFields()) {
            field.accept(this);
        }
        exitCurrentParent();
    }

    @Override
    public void visit(PrimitiveType primitiveType) {
        ParquetSchemaNode node = new ParquetSchemaNode(
                primitiveType.getName(),
                ParquetSchemaType.PRIMITIVE,
                primitiveType.getPrimitiveTypeName()
        );
        attachToCurrentParent(node);
    }

    /**
     * @return The extracted {@link ParquetSchemaNode} schema representation.
     */
    public ParquetSchemaNode getSchema() {
        return root;
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
