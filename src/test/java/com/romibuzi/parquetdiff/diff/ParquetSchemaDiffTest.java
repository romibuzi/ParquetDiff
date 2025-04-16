package com.romibuzi.parquetdiff.diff;

import com.romibuzi.parquetdiff.TestUtils;
import com.romibuzi.parquetdiff.junit.CaptureSystemOut;
import com.romibuzi.parquetdiff.junit.CapturedSystemOut;
import com.romibuzi.parquetdiff.metadata.ParquetDetails;
import com.romibuzi.parquetdiff.metadata.ParquetSchemaType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class ParquetSchemaDiffTest {
    private ParquetSchemaDiff diff;

    @BeforeEach
    void setUp() {
        ParquetDetails first = TestUtils.generateParquetDetails(null);
        ParquetDetails second = TestUtils.generateParquetDetails(null);
        diff = new ParquetSchemaDiff(first, second);
    }

    @Test
    void hasNoDifferences() {
        assertFalse(diff.hasDifferences());
        assertTrue(diff.additionalNodes().isEmpty());
        assertTrue(diff.missingNodes().isEmpty());
        assertTrue(diff.typeDiffs().isEmpty());
        assertTrue(diff.primitiveTypeDiffs().isEmpty());
    }

    @Test
    void additionalNode() {
        diff.addAdditionalNode("name");
        diff.addAdditionalNode("email");
        assertTrue(diff.hasDifferences());
        assertEquals(List.of("name", "email"), diff.additionalNodes());
    }

    @Test
    void missingNode() {
        diff.addMissingNode("name");
        diff.addMissingNode("email");
        assertTrue(diff.hasDifferences());
        assertEquals(List.of("name", "email"), diff.missingNodes());
    }

    @Test
    void typeDiff() {
        ParquetSchemaTypeDiff first = new ParquetSchemaTypeDiff("name", ParquetSchemaType.PRIMITIVE,
                ParquetSchemaType.GROUP);
        ParquetSchemaTypeDiff second = new ParquetSchemaTypeDiff("email", ParquetSchemaType.PRIMITIVE,
                ParquetSchemaType.GROUP);
        diff.addTypeDiff(first);
        diff.addTypeDiff(second);
        assertTrue(diff.hasDifferences());
        assertEquals(List.of(first, second), diff.typeDiffs());
    }

    @Test
    void primitiveTypeDiff() {
        ParquetSchemaPrimitiveTypeDiff first = new ParquetSchemaPrimitiveTypeDiff("name",
                PrimitiveType.PrimitiveTypeName.BINARY, PrimitiveType.PrimitiveTypeName.INT32);
        ParquetSchemaPrimitiveTypeDiff second = new ParquetSchemaPrimitiveTypeDiff("email",
                PrimitiveType.PrimitiveTypeName.INT32, PrimitiveType.PrimitiveTypeName.BINARY);
        diff.addPrimitiveTypeDiff(first);
        diff.addPrimitiveTypeDiff(second);
        assertTrue(diff.hasDifferences());
        assertEquals(List.of(first, second), diff.primitiveTypeDiffs());
    }

    @Test
    @CaptureSystemOut
    void printNoDifferences(@CapturedSystemOut Supplier<String> capturedOut) {
        diff.printDifferences();
        String[] lines = TestUtils.getLines(capturedOut);
        assertEquals(1, lines.length);
        assertTrue(lines[0].contains("No differences found"));
    }

    @Test
    @CaptureSystemOut
    void printDifferencesAdditionalNode(@CapturedSystemOut Supplier<String> capturedOut) {
        diff.addAdditionalNode("name");
        diff.addAdditionalNode("email");
        diff.printDifferences();
        String[] lines = TestUtils.getLines(capturedOut);
        assertEquals(3, lines.length);
        assertTrue(lines[0].contains("Differences found"));
        assertEquals("additional field: 'name'.", lines[1]);
        assertEquals("additional field: 'email'.", lines[2]);
    }

    @Test
    @CaptureSystemOut
    void printDifferencesMissingNode(@CapturedSystemOut Supplier<String> capturedOut) {
        diff.addMissingNode("name");
        diff.addMissingNode("email");
        diff.printDifferences();
        String[] lines = TestUtils.getLines(capturedOut);
        assertEquals(3, lines.length);
        assertTrue(lines[0].contains("Differences found"));
        assertEquals("missing field: 'name'.", lines[1]);
        assertEquals("missing field: 'email'.", lines[2]);
    }

    @Test
    @CaptureSystemOut
    void printDifferencesTypeDiff(@CapturedSystemOut Supplier<String> capturedOut) {
        ParquetSchemaTypeDiff first = new ParquetSchemaTypeDiff("name", ParquetSchemaType.PRIMITIVE,
                ParquetSchemaType.GROUP);
        ParquetSchemaTypeDiff second = new ParquetSchemaTypeDiff("email", ParquetSchemaType.GROUP,
                ParquetSchemaType.PRIMITIVE);
        diff.addTypeDiff(first);
        diff.addTypeDiff(second);
        diff.printDifferences();
        String[] lines = TestUtils.getLines(capturedOut);
        assertEquals(3, lines.length);
        assertTrue(lines[0].contains("Differences found"));
        assertEquals("different field type for 'name': 'struct' instead of 'primitive'.", lines[1]);
        assertEquals("different field type for 'email': 'primitive' instead of 'struct'.", lines[2]);
    }

    @Test
    @CaptureSystemOut
    void printDifferencesPrimitiveTypeDiff(@CapturedSystemOut Supplier<String> capturedOut) {
        ParquetSchemaPrimitiveTypeDiff first = new ParquetSchemaPrimitiveTypeDiff("name",
                PrimitiveType.PrimitiveTypeName.BINARY, PrimitiveType.PrimitiveTypeName.INT32);
        ParquetSchemaPrimitiveTypeDiff second = new ParquetSchemaPrimitiveTypeDiff("email",
                PrimitiveType.PrimitiveTypeName.INT32, PrimitiveType.PrimitiveTypeName.BINARY);
        diff.addPrimitiveTypeDiff(first);
        diff.addPrimitiveTypeDiff(second);
        diff.printDifferences();
        String[] lines = TestUtils.getLines(capturedOut);
        assertEquals(3, lines.length);
        assertTrue(lines[0].contains("Differences found"));
        assertEquals("different field primitive type for 'name': 'INT32' instead of 'BINARY'.", lines[1]);
        assertEquals("different field primitive type for 'email': 'BINARY' instead of 'INT32'.", lines[2]);
    }
}
