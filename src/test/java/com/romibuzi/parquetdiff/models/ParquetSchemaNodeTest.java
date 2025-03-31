package com.romibuzi.parquetdiff.models;

import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ParquetSchemaNodeTest {
    @Test
    void addChild() {
        ParquetSchemaNode node = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null);
        ParquetSchemaNode child = new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.BINARY);

        node.addChild(child);

        assertTrue(node.hasChildren());
        assertEquals(child, node.children().get(0));
        assertFalse(child.hasChildren());
    }

    @Test
    void addChildPrimitive() {
        ParquetSchemaNode node = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.INT32);
        ParquetSchemaNode child = new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.BINARY);

        UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> node.addChild(child));
        assertEquals("PRIMITIVE field can't have child field", exception.getMessage());
    }
}
