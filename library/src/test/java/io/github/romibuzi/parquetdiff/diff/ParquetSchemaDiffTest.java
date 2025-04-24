package io.github.romibuzi.parquetdiff.diff;

import io.github.romibuzi.parquetdiff.TestUtils;
import io.github.romibuzi.parquetdiff.metadata.ParquetDetails;
import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaNodePath;
import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ParquetSchemaDiffTest {
    private ParquetSchemaDiff diff;
    private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    private final PrintStream outputPrintStream = new PrintStream(outputStream);

    @BeforeEach
    void setUp() {
        ParquetDetails first = TestUtils.generateParquetDetails(null);
        ParquetDetails second = TestUtils.generateParquetDetails(null);
        diff = new ParquetSchemaDiff(first, second);

    }

    @Test
    void hasNoDifferences() {
        assertFalse(diff.hasDifferences());
        assertTrue(diff.getAdditionalNodes().isEmpty());
        assertTrue(diff.getMissingNodes().isEmpty());
        assertTrue(diff.getTypeDiffs().isEmpty());
        assertTrue(diff.getPrimitiveTypeDiffs().isEmpty());
    }

    @Test
    void additionalNode() {
        diff.addAdditionalNode(new ParquetSchemaNodePath("name"));
        diff.addAdditionalNode(new ParquetSchemaNodePath("email"));
        assertTrue(diff.hasDifferences());
        assertEquals(List.of(new ParquetSchemaNodePath("name"), new ParquetSchemaNodePath("email")),
                diff.getAdditionalNodes());
    }

    @Test
    void missingNode() {
        diff.addMissingNode(new ParquetSchemaNodePath("name"));
        diff.addMissingNode(new ParquetSchemaNodePath("email"));
        assertTrue(diff.hasDifferences());
        assertEquals(List.of(new ParquetSchemaNodePath("name"), new ParquetSchemaNodePath("email")),
                diff.getMissingNodes());
    }

    @Test
    void typeDiff() {
        ParquetSchemaTypeDiff first = new ParquetSchemaTypeDiff(new ParquetSchemaNodePath("name"),
                ParquetSchemaType.PRIMITIVE,
                ParquetSchemaType.GROUP);
        ParquetSchemaTypeDiff second = new ParquetSchemaTypeDiff(new ParquetSchemaNodePath("email"),
                ParquetSchemaType.PRIMITIVE,
                ParquetSchemaType.GROUP);
        diff.addTypeDiff(first);
        diff.addTypeDiff(second);
        assertTrue(diff.hasDifferences());
        assertEquals(List.of(first, second), diff.getTypeDiffs());
    }

    @Test
    void primitiveTypeDiff() {
        ParquetSchemaPrimitiveTypeDiff first = new ParquetSchemaPrimitiveTypeDiff(new ParquetSchemaNodePath("name"),
                PrimitiveType.PrimitiveTypeName.BINARY, PrimitiveType.PrimitiveTypeName.INT32);
        ParquetSchemaPrimitiveTypeDiff second = new ParquetSchemaPrimitiveTypeDiff(new ParquetSchemaNodePath("email"),
                PrimitiveType.PrimitiveTypeName.INT32, PrimitiveType.PrimitiveTypeName.BINARY);
        diff.addPrimitiveTypeDiff(first);
        diff.addPrimitiveTypeDiff(second);
        assertTrue(diff.hasDifferences());
        assertEquals(List.of(first, second), diff.getPrimitiveTypeDiffs());
    }

    @Test
    void printNoDifferences() {
        diff.print(outputPrintStream);
        String[] lines = TestUtils.getLines(outputStream.toString());
        assertEquals(1, lines.length);
        assertTrue(lines[0].contains("No differences found"));
    }

    @Test
    void printAdditionalNode() {
        diff.addAdditionalNode(new ParquetSchemaNodePath("name"));
        diff.addAdditionalNode(new ParquetSchemaNodePath("email"));
        diff.print(outputPrintStream);
        String[] lines = TestUtils.getLines(outputStream.toString());
        assertEquals(3, lines.length);
        assertTrue(lines[0].contains("Differences found"));
        assertEquals("additional field: 'name'.", lines[1]);
        assertEquals("additional field: 'email'.", lines[2]);
    }

    @Test
    void printMissingNode() {
        diff.addMissingNode(new ParquetSchemaNodePath("name"));
        diff.addMissingNode(new ParquetSchemaNodePath("email"));
        diff.print(outputPrintStream);
        String[] lines = TestUtils.getLines(outputStream.toString());
        assertEquals(3, lines.length);
        assertTrue(lines[0].contains("Differences found"));
        assertEquals("missing field: 'name'.", lines[1]);
        assertEquals("missing field: 'email'.", lines[2]);
    }

    @Test
    void printTypeDiff() {
        ParquetSchemaTypeDiff first = new ParquetSchemaTypeDiff(new ParquetSchemaNodePath("name"),
                ParquetSchemaType.PRIMITIVE,
                ParquetSchemaType.GROUP);
        ParquetSchemaTypeDiff second = new ParquetSchemaTypeDiff(new ParquetSchemaNodePath("email"),
                ParquetSchemaType.GROUP,
                ParquetSchemaType.PRIMITIVE);
        diff.addTypeDiff(first);
        diff.addTypeDiff(second);
        diff.print(outputPrintStream);
        String[] lines = TestUtils.getLines(outputStream.toString());
        assertEquals(3, lines.length);
        assertTrue(lines[0].contains("Differences found"));
        assertEquals("different field type for 'name': 'struct' instead of 'primitive'.", lines[1]);
        assertEquals("different field type for 'email': 'primitive' instead of 'struct'.", lines[2]);
    }

    @Test
    void printPrimitiveTypeDiff() {
        ParquetSchemaPrimitiveTypeDiff first = new ParquetSchemaPrimitiveTypeDiff(new ParquetSchemaNodePath("name"),
                PrimitiveType.PrimitiveTypeName.BINARY, PrimitiveType.PrimitiveTypeName.INT32);
        ParquetSchemaPrimitiveTypeDiff second = new ParquetSchemaPrimitiveTypeDiff(new ParquetSchemaNodePath("email"),
                PrimitiveType.PrimitiveTypeName.INT32, PrimitiveType.PrimitiveTypeName.BINARY);
        diff.addPrimitiveTypeDiff(first);
        diff.addPrimitiveTypeDiff(second);
        diff.print(outputPrintStream);
        String[] lines = TestUtils.getLines(outputStream.toString());
        assertEquals(3, lines.length);
        assertTrue(lines[0].contains("Differences found"));
        assertEquals("different field primitive type for 'name': 'INT32' instead of 'BINARY'.", lines[1]);
        assertEquals("different field primitive type for 'email': 'BINARY' instead of 'INT32'.", lines[2]);
    }
}
