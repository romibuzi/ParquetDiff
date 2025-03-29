package com.romibuzi.parquetdiff;

import com.romibuzi.parquetdiff.models.ParquetSchemaNode;
import com.romibuzi.parquetdiff.models.ParquetSchemaType;
import org.apache.parquet.schema.*;

import java.util.Stack;

public class ParquetTypeVisitor implements TypeVisitor {
    private ParquetSchemaNode root;
    private final Stack<ParquetSchemaNode> nodeStack = new Stack<>();

    @Override
    public void visit(MessageType messageType) {
        root = new ParquetSchemaNode(messageType.getName(), ParquetSchemaType.MESSAGE, null);
        nodeStack.push(root);
        for (Type field : messageType.getFields()) {
            field.accept(this);
        }
        nodeStack.pop();
    }

    @Override
    public void visit(GroupType groupType) {
        ParquetSchemaNode node = new ParquetSchemaNode(groupType.getName(), ParquetSchemaType.GROUP, null);
        nodeStack.peek().addChild(node);
        nodeStack.push(node);
        for (Type field : groupType.getFields()) {
            field.accept(this);
        }
        nodeStack.pop();
    }

    @Override
    public void visit(PrimitiveType primitiveType) {
        ParquetSchemaNode node = new ParquetSchemaNode(
                primitiveType.getName(),
                ParquetSchemaType.PRIMITIVE,
                primitiveType.getPrimitiveTypeName()
        );
        nodeStack.peek().addChild(node);
    }

    public ParquetSchemaNode getSchema() {
        return root;
    }
}
