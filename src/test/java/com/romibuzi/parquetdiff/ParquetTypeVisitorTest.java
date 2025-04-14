package com.romibuzi.parquetdiff;

import com.romibuzi.parquetdiff.models.ParquetSchemaNode;
import com.romibuzi.parquetdiff.models.ParquetSchemaType;
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
        String schemaInput = """
                message test_schema {
                    required int32 id;
                }
                """;

        ParquetSchemaNode schema = extractSchema(schemaInput);
        ParquetSchemaNode expected = new ParquetSchemaNode("root", ParquetSchemaType.MESSAGE, null, List.of(
                new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE, PrimitiveType.PrimitiveTypeName.INT32)
        ));

        assertEquals(expected, schema);
    }

    @Test
    void visitSchemaWithListField() {
        String schemaInput = """
                message test_schema {
                    repeated group list_field (LIST) {
                        required binary value (STRING);
                    }
                }
                """;

        ParquetSchemaNode schema = extractSchema(schemaInput);
        ParquetSchemaNode expected = new ParquetSchemaNode("root", ParquetSchemaType.MESSAGE, null, List.of(
                new ParquetSchemaNode("list_field", ParquetSchemaType.LIST, null, List.of(
                        new ParquetSchemaNode("value", ParquetSchemaType.PRIMITIVE,
                                PrimitiveType.PrimitiveTypeName.BINARY)
                ))
        ));

        assertEquals(expected, schema);
    }

    @Test
    void visitSchemaWithMapField() {
        String schemaInput = """
                message test_schema {
                    repeated group map_field (MAP) {
                        required binary key (STRING);
                        optional int32 value;
                    }
                }
                """;

        ParquetSchemaNode schema = extractSchema(schemaInput);
        ParquetSchemaNode expected = new ParquetSchemaNode("root", ParquetSchemaType.MESSAGE, null, List.of(
                new ParquetSchemaNode("map_field", ParquetSchemaType.MAP, null, List.of(
                        new ParquetSchemaNode("key", ParquetSchemaType.PRIMITIVE,
                                PrimitiveType.PrimitiveTypeName.BINARY),
                        new ParquetSchemaNode("value", ParquetSchemaType.PRIMITIVE,
                                PrimitiveType.PrimitiveTypeName.INT32)
                ))
        ));

        assertEquals(expected, schema);
    }

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

        ParquetSchemaNode schema = extractSchema(schemaInput);
        ParquetSchemaNode expected = new ParquetSchemaNode(
                "root",
                ParquetSchemaType.MESSAGE,
                null,
                List.of(
                        new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE, PrimitiveType.PrimitiveTypeName.INT32),
                        new ParquetSchemaNode("address", ParquetSchemaType.GROUP, null, List.of(
                                new ParquetSchemaNode("street", ParquetSchemaType.PRIMITIVE,
                                        PrimitiveType.PrimitiveTypeName.BINARY),
                                new ParquetSchemaNode("zip_code", ParquetSchemaType.PRIMITIVE,
                                        PrimitiveType.PrimitiveTypeName.INT64)
                        )),
                        new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE,
                                PrimitiveType.PrimitiveTypeName.BINARY)
                ));

        assertEquals(expected, schema);
    }

    @Test
    void visitNestedSchema() {
        String schemaInput = """
                message test_schema {
                    required group address {
                        required binary street (UTF8);
                        required group tenant {
                          required binary name (UTF8);
                        }
                    }
                }
                """;

        ParquetSchemaNode schema = extractSchema(schemaInput);
        ParquetSchemaNode expected = new ParquetSchemaNode(
                "root",
                ParquetSchemaType.MESSAGE,
                null,
                List.of(
                        new ParquetSchemaNode("address", ParquetSchemaType.GROUP, null, List.of(
                                new ParquetSchemaNode("street", ParquetSchemaType.PRIMITIVE,
                                        PrimitiveType.PrimitiveTypeName.BINARY),
                                new ParquetSchemaNode("tenant", ParquetSchemaType.GROUP, null, List.of(
                                        new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE,
                                                PrimitiveType.PrimitiveTypeName.BINARY)
                                ))
                        ))
                ));

        assertEquals(expected, schema);
    }

    @Test
    void visitEmptySchema() {
        String schemaInput = "message test_schema {}";

        ParquetSchemaNode schema = extractSchema(schemaInput);
        ParquetSchemaNode expected = new ParquetSchemaNode("root", ParquetSchemaType.MESSAGE, null);

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
