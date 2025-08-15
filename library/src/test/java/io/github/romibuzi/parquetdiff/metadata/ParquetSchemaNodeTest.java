package io.github.romibuzi.parquetdiff.metadata;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.jupiter.api.Assertions.*;

class ParquetSchemaNodeTest {
    @Test
    void addChild() {
        ParquetSchemaNode node = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, REQUIRED, null, null);
        ParquetSchemaNode child = new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());

        node.addChild(child);

        assertTrue(node.hasChildren());
        assertEquals(child, node.getChildren().get(0));
        assertFalse(child.hasChildren());
    }

    @Test
    void addChildPrimitive() {
        ParquetSchemaNode node = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT32, null);
        ParquetSchemaNode child = new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());

        UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> node.addChild(child));
        assertEquals("Primitive field can't have children", exception.getMessage());
    }

    @Test
    void primitiveName() {
        ParquetSchemaNode node = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT32, null);
        assertEquals("int32", node.primitiveName());
    }

    @Test
    void primitiveNameLogicalType() {
        ParquetSchemaNode node = new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());
        assertEquals("string (binary)", node.primitiveName());
    }

    @Test
    void print() {
        ParquetSchemaNode node = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, REQUIRED, null, null,
                List.of(
                        new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE, REQUIRED,
                                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()),
                        new ParquetSchemaNode("address", ParquetSchemaType.GROUP, REQUIRED, null, null, List.of(
                                new ParquetSchemaNode("street", ParquetSchemaType.PRIMITIVE, REQUIRED,
                                        PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType())))
                ));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outputStream);

        node.print(printStream);

        String expected = String.join("\n",
                "test_schema:",
                "  |-- name: string (binary)",
                "  |-- address: struct",
                "    |-- street: string (binary)"
        );
        String output = outputStream.toString().trim();

        assertEquals(expected, output);
    }
}
