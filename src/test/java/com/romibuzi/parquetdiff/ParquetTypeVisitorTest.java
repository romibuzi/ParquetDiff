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

        parseSchema(schemaInput).accept(visitor);
        ParquetSchemaNode schema = visitor.getSchema();

        ParquetSchemaNode expected = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null, List.of(
                new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE, PrimitiveType.PrimitiveTypeName.INT32)
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

        parseSchema(schemaInput).accept(visitor);
        ParquetSchemaNode schema = visitor.getSchema();

        ParquetSchemaNode expected = new ParquetSchemaNode(
                "test_schema",
                ParquetSchemaType.MESSAGE,
                null,
                List.of(
                        new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE, PrimitiveType.PrimitiveTypeName.INT32),
                        new ParquetSchemaNode("address", ParquetSchemaType.GROUP, null, List.of(
                                new ParquetSchemaNode("street", ParquetSchemaType.PRIMITIVE, PrimitiveType.PrimitiveTypeName.BINARY),
                                new ParquetSchemaNode("zip_code", ParquetSchemaType.PRIMITIVE, PrimitiveType.PrimitiveTypeName.INT64)
                        )),
                        new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE, PrimitiveType.PrimitiveTypeName.BINARY)
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

        parseSchema(schemaInput).accept(visitor);
        ParquetSchemaNode schema = visitor.getSchema();

        ParquetSchemaNode expected = new ParquetSchemaNode(
                "test_schema",
                ParquetSchemaType.MESSAGE,
                null,
                List.of(
                        new ParquetSchemaNode("address", ParquetSchemaType.GROUP, null, List.of(
                                new ParquetSchemaNode("street", ParquetSchemaType.PRIMITIVE, PrimitiveType.PrimitiveTypeName.BINARY),
                                new ParquetSchemaNode("tenant", ParquetSchemaType.GROUP, null, List.of(
                                        new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE, PrimitiveType.PrimitiveTypeName.BINARY)
                                ))
                        ))
                ));

        assertEquals(expected, schema);
    }

    @Test
    void visitEmptySchema() {
        String schemaInput = "message test_schema {}";

        parseSchema(schemaInput).accept(visitor);
        ParquetSchemaNode schema = visitor.getSchema();

        ParquetSchemaNode expected = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null);

        assertEquals(expected, schema);
    }

    private MessageType parseSchema(String schemaInput) {
        return MessageTypeParser.parseMessageType(schemaInput);
    }
}
