package io.github.romibuzi.parquetdiff;

import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaNode;
import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ParquetTypeVisitorTest {
    private final ParquetTypeVisitor visitor = new ParquetTypeVisitor();

    @Test
    void visitSchemaSingleField() {
        String schemaInput = "message test_schema { required int32 id; }";

        ParquetSchemaNode schema = extractSchema(schemaInput);
        ParquetSchemaNode expected = new ParquetSchemaNode("root", ParquetSchemaType.MESSAGE, null, null, List.of(
                new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE, PrimitiveType.PrimitiveTypeName.INT32, null)
        ));

        assertEquals(expected, schema);
    }

    @Test
    void visitSchemaWithListField() {
        String schemaInput = "message test_schema {\n" +
                "   repeated group list_field (LIST) {\n" +
                "       required binary value (STRING);\n" +
                "   }\n" +
                "}";

        ParquetSchemaNode schema = extractSchema(schemaInput);
        ParquetSchemaNode expected = new ParquetSchemaNode("root", ParquetSchemaType.MESSAGE, null, null, List.of(
                new ParquetSchemaNode("list_field", ParquetSchemaType.LIST, null, null, List.of(
                        new ParquetSchemaNode("value", ParquetSchemaType.PRIMITIVE,
                                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType())
                ))
        ));

        assertEquals(expected, schema);
    }

    @Test
    void visitSchemaWithMapField() {
        String schemaInput = "message test_schema {\n" +
                "   repeated group map_field (MAP) {\n" +
                "       required binary key (STRING);\n" +
                "       optional int32 value;\n" +
                "   }\n" +
                "}";

        ParquetSchemaNode schema = extractSchema(schemaInput);
        ParquetSchemaNode expected = new ParquetSchemaNode("root", ParquetSchemaType.MESSAGE, null, null, List.of(
                new ParquetSchemaNode("map_field", ParquetSchemaType.MAP, null, null, List.of(
                        new ParquetSchemaNode("key", ParquetSchemaType.PRIMITIVE,
                                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()),
                        new ParquetSchemaNode("value", ParquetSchemaType.PRIMITIVE,
                                PrimitiveType.PrimitiveTypeName.INT32, null)
                ))
        ));

        assertEquals(expected, schema);
    }

    @Test
    void visitSchema() {
        String schemaInput = "message test_schema {\n" +
                "    required int32 id;\n" +
                "    required group address {\n" +
                "        required binary street (UTF8);\n" +
                "        required int64 zip_code;\n" +
                "    }\n" +
                "    optional binary name (UTF8);\n" +
                "}";

        ParquetSchemaNode schema = extractSchema(schemaInput);
        ParquetSchemaNode expected = new ParquetSchemaNode(
                "root",
                ParquetSchemaType.MESSAGE,
                null,
                null,
                List.of(
                        new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                                PrimitiveType.PrimitiveTypeName.INT32, null),
                        new ParquetSchemaNode("address", ParquetSchemaType.GROUP, null, null, List.of(
                                new ParquetSchemaNode("street", ParquetSchemaType.PRIMITIVE,
                                        PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()),
                                new ParquetSchemaNode("zip_code", ParquetSchemaType.PRIMITIVE,
                                        PrimitiveType.PrimitiveTypeName.INT64, null)
                        )),
                        new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE,
                                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType())
                ));

        assertEquals(expected, schema);
    }

    @Test
    void visitNestedSchema() {
        String schemaInput = "message test_schema {\n" +
                "    required group address {\n" +
                "        required binary street (UTF8);\n" +
                "        required group tenant {\n" +
                "          required binary name (UTF8);\n" +
                "        }\n" +
                "    }\n" +
                "}";

        ParquetSchemaNode schema = extractSchema(schemaInput);
        ParquetSchemaNode expected = new ParquetSchemaNode(
                "root",
                ParquetSchemaType.MESSAGE,
                null,
                null,
                List.of(
                        new ParquetSchemaNode("address", ParquetSchemaType.GROUP, null, null, List.of(
                                new ParquetSchemaNode("street", ParquetSchemaType.PRIMITIVE,
                                        PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType()),
                                new ParquetSchemaNode("tenant", ParquetSchemaType.GROUP, null, null, List.of(
                                        new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE,
                                                PrimitiveType.PrimitiveTypeName.BINARY,
                                                LogicalTypeAnnotation.stringType())
                                ))
                        ))
                ));

        assertEquals(expected, schema);
    }

    @Test
    void visitEmptySchema() {
        String schemaInput = "message test_schema {}";

        ParquetSchemaNode schema = extractSchema(schemaInput);
        ParquetSchemaNode expected = new ParquetSchemaNode("root", ParquetSchemaType.MESSAGE, null, null);

        assertEquals(expected, schema);
    }

    private ParquetSchemaNode extractSchema(String schemaInput) {
        readSchema(schemaInput).accept(visitor);
        return visitor.getSchema();
    }

    private MessageType readSchema(String schemaInput) {
        return MessageTypeParser.parseMessageType(schemaInput);
    }
}
