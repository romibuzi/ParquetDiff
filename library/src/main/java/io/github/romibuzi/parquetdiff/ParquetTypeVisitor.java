package io.github.romibuzi.parquetdiff;

import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaNode;
import org.apache.parquet.schema.*;

import java.util.Stack;

/**
 * A TypeVisitor implementation to extract a given {@link MessageType} into a {@link ParquetSchemaNode}.
 */
final class ParquetTypeVisitor implements TypeVisitor {
    private ParquetSchemaNode root;
    private final Stack<ParquetSchemaNode> nodeStack = new Stack<>();

    @Override
    public void visit(MessageType messageType) {
        root = ParquetSchemaNodeConverter.fromMessageType(messageType);
        enterNewParent(root);
        visitChildren(messageType);
        exitCurrentParent();
    }

    @Override
    public void visit(GroupType groupType) {
        ParquetSchemaNode node = ParquetSchemaNodeConverter.fromGroupType(groupType);
        attachToCurrentParent(node);
        enterNewParent(node);
        visitChildren(groupType);
        exitCurrentParent();
    }

    @Override
    public void visit(PrimitiveType primitiveType) {
        ParquetSchemaNode node = ParquetSchemaNodeConverter.fromPrimitiveType(primitiveType);
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

    private void visitChildren(GroupType groupType) {
        for (Type field : groupType.getFields()) {
            field.accept(this);
        }
    }
}
