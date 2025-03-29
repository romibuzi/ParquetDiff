package com.romibuzi.parquetdiff;

import com.romibuzi.parquetdiff.models.ParquetSchemaNode;
import com.romibuzi.parquetdiff.models.ParquetSchemaType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ParquetTypeVisitorTest {
    @Test
    void visitSchema() {
        String schemaInput = """
                message test_schema {
                    required int32 id;
                    required group address {
                        required binary street (UTF8);
                        required int64 zip_code;
                    }
                    optional binary name (UTF8);
                }
                """;

        MessageType messageType = MessageTypeParser.parseMessageType(schemaInput);

        ParquetTypeVisitor visitor = new ParquetTypeVisitor();
        messageType.accept(visitor);

        ParquetSchemaNode schema = visitor.getSchema();

        assertEquals("test_schema", schema.name());
        assertEquals(ParquetSchemaType.MESSAGE, schema.type());
        assertNull(schema.primitiveTypeName());
        assertEquals(3, schema.children().size());

        ParquetSchemaNode firstChild = schema.children().get(0);
        assertEquals("id", firstChild.name());
        assertEquals(ParquetSchemaType.PRIMITIVE, firstChild.type());
        assertEquals(PrimitiveType.PrimitiveTypeName.INT32, firstChild.primitiveTypeName());
        assertTrue(firstChild.children().isEmpty());

        ParquetSchemaNode secondChild = schema.children().get(1);
        assertEquals("address", secondChild.name());
        assertEquals(ParquetSchemaType.GROUP, secondChild.type());
        assertNull(secondChild.primitiveTypeName());
        assertEquals(2, secondChild.children().size());
        assertEquals("street", secondChild.children().get(0).name());
        assertEquals(ParquetSchemaType.PRIMITIVE, secondChild.children().get(0).type());
        assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, secondChild.children().get(0).primitiveTypeName());
        assertEquals("zip_code", secondChild.children().get(1).name());
        assertEquals(ParquetSchemaType.PRIMITIVE, secondChild.children().get(1).type());
        assertEquals(PrimitiveType.PrimitiveTypeName.INT64, secondChild.children().get(1).primitiveTypeName());

        ParquetSchemaNode thirdChild = schema.children().get(2);
        assertEquals("name", thirdChild.name());
        assertEquals(ParquetSchemaType.PRIMITIVE, thirdChild.type());
        assertEquals(PrimitiveType.PrimitiveTypeName.BINARY, thirdChild.primitiveTypeName());
        assertTrue(thirdChild.children().isEmpty());
    }

    @Test
    void visitEmptySchema() {
        String schemaInput = "message test_schema {}";

        MessageType messageType = MessageTypeParser.parseMessageType(schemaInput);

        ParquetTypeVisitor visitor = new ParquetTypeVisitor();
        messageType.accept(visitor);

        ParquetSchemaNode schema = visitor.getSchema();

        assertEquals("test_schema", schema.name());
        assertEquals(ParquetSchemaType.MESSAGE, schema.type());
        assertTrue(schema.children().isEmpty());
    }
}
