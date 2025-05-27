package io.github.romibuzi.parquetdiff.diff;

import io.github.romibuzi.parquetdiff.TestUtils;
import io.github.romibuzi.parquetdiff.metadata.*;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static io.github.romibuzi.parquetdiff.metadata.ParquetSchemaType.MESSAGE;
import static io.github.romibuzi.parquetdiff.metadata.ParquetSchemaType.PRIMITIVE;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.jupiter.api.Assertions.*;

class ParquetComparatorTest {
    @Test
    void findDifferentPartitionsStructureEmpty() {
        List<ParquetPartitions> results = ParquetComparator.findDifferentPartitionsStructure(Collections.emptyList());
        assertTrue(results.isEmpty());
    }

    @Test
    void findDifferentPartitionsStructureSinglePartition() {
        String path = "vaccinations.parquet/date=2020-12-28/country=Spain/part-0000.parquet";
        ParquetDetails first = new ParquetDetails(new Path(path), 10, null);
        List<ParquetPartitions> results = ParquetComparator.findDifferentPartitionsStructure(List.of(first));
        assertTrue(results.isEmpty());
    }

    @Test
    void findDifferentPartitionsStructureAllSame() {
        String firstPath = "vaccinations.parquet/date=2020-12-28/country=Spain/part-0000.parquet";
        String secondPath = "vaccinations.parquet/date=2020-12-29/country=Spain/part-0000.parquet";
        ParquetDetails first = new ParquetDetails(new Path(firstPath), 10, null);
        ParquetDetails second = new ParquetDetails(new Path(secondPath), 10, null);
        List<ParquetPartitions> results = ParquetComparator.findDifferentPartitionsStructure(List.of(first, second));
        assertTrue(results.isEmpty());
    }

    @Test
    void findDifferentPartitionsStructure() {
        String firstPath = "vaccinations.parquet/date=2020-12-28/country=Spain/part-0000.parquet";
        String secondPath = "vaccinations.parquet/date=2020-12-29/part-0000.parquet";
        ParquetDetails first = new ParquetDetails(new Path(firstPath), 10, null);
        ParquetDetails second = new ParquetDetails(new Path(secondPath), 10, null);
        List<ParquetPartitions> results = ParquetComparator.findDifferentPartitionsStructure(List.of(first, second));
        assertEquals(2, results.size());
        assertEquals(List.of("date", "country"), results.get(0).getKeys());
        assertEquals(List.of("date"), results.get(1).getKeys());
    }

    @Test
    void compareSchemasIdentical() {
        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED,
                null, null);
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED,
                null, null);

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertFalse(result.hasDifferences());
    }

    @Test
    void compareSchemasIdenticalWithFields() {
        ParquetSchemaNode id = new ParquetSchemaNode("id", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT32, null);
        ParquetSchemaNode name = new ParquetSchemaNode("name", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED, null,
                null, List.of(id, name));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED, null,
                null, List.of(id, name));

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertFalse(result.hasDifferences());
    }

    @Test
    void compareSchemasAdditionalField() {
        ParquetSchemaNode id = new ParquetSchemaNode("id", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT32, null);
        ParquetSchemaNode name = new ParquetSchemaNode("name", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED, null,
                null, List.of(id));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED, null,
                null, List.of(id, name));

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of(new ParquetSchemaNodePath("test_schema", "name")), result.getAdditionalNodes());
    }

    @Test
    void compareSchemasMissingField() {
        ParquetSchemaNode id = new ParquetSchemaNode("id", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT32, null);
        ParquetSchemaNode name = new ParquetSchemaNode("name", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED, null,
                null, List.of(id, name));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED, null,
                null, List.of(id));

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of(new ParquetSchemaNodePath("test_schema", "name")), result.getMissingNodes());
    }

    @Test
    void compareSchemasDifferentFieldType() {
        ParquetSchemaNode id32 = new ParquetSchemaNode("id", ParquetSchemaType.GROUP, REQUIRED, null, null);
        ParquetSchemaNode id64 = new ParquetSchemaNode("id", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT64, null);

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED, null,
                null, List.of(id32));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED, null,
                null, List.of(id64));

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of(new ParquetSchemaTypeDiff(new ParquetSchemaNodePath("test_schema", "id"),
                ParquetSchemaType.GROUP,
                PRIMITIVE)), result.getTypeDiffs());
    }

    @Test
    void compareSchemasDifferentPrimitiveFieldType() {
        ParquetSchemaNode id32 = new ParquetSchemaNode("id", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT32, null);
        ParquetSchemaNode id64 = new ParquetSchemaNode("id", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT64, null);

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED, null,
                null, List.of(id32));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED, null,
                null, List.of(id64));

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of(new ParquetSchemaPrimitiveTypeDiff(new ParquetSchemaNodePath("test_schema", "id"),
                        PrimitiveType.PrimitiveTypeName.INT32, PrimitiveType.PrimitiveTypeName.INT64)),
                result.getPrimitiveTypeDiffs());
    }

    @Test
    void compareSchemasAdditionalAndMissingField() {
        ParquetSchemaNode id = new ParquetSchemaNode("id", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT32, null);
        ParquetSchemaNode name = new ParquetSchemaNode("name", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());
        ParquetSchemaNode email = new ParquetSchemaNode("email", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED, null,
                null, List.of(id, name));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED, null,
                null, List.of(id, email));

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of(new ParquetSchemaNodePath("test_schema", "name")), result.getMissingNodes());
        assertEquals(List.of(new ParquetSchemaNodePath("test_schema", "email")), result.getAdditionalNodes());
    }

    @Test
    void compareSchemasAdditionalAndMissingFieldNested() {
        ParquetSchemaNode street = new ParquetSchemaNode("street", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());
        ParquetSchemaNode zipCode = new ParquetSchemaNode("zip_code", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());
        ParquetSchemaNode city = new ParquetSchemaNode("city", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());

        ParquetSchemaNode firstAddress = new ParquetSchemaNode("address", ParquetSchemaType.GROUP, REQUIRED, null,
                null, List.of(street, zipCode));
        ParquetSchemaNode secondAddress = new ParquetSchemaNode("address", ParquetSchemaType.GROUP, REQUIRED, null,
                null, List.of(street, city));

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED, null,
                null, List.of(firstAddress));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED, null,
                null, List.of(secondAddress));

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of(new ParquetSchemaNodePath("test_schema", "address", "zip_code")),
                result.getMissingNodes());
        assertEquals(List.of(new ParquetSchemaNodePath("test_schema", "address", "city")), result.getAdditionalNodes());
    }

    @Test
    void compareSchemasAdditionalAndMissingFieldDeeplyNested() {
        ParquetSchemaNode id32 = new ParquetSchemaNode("id", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT32, null);
        ParquetSchemaNode name = new ParquetSchemaNode("name", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());

        int depth = 5;
        ParquetSchemaNode firstSchema = createNestedSchema("root", id32, depth);
        ParquetSchemaNode secondSchema = createNestedSchema("root", name, depth);

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of(new ParquetSchemaNodePath("root", "4", "3", "2", "1", "id")), result.getMissingNodes());
        assertEquals(List.of(new ParquetSchemaNodePath("root", "4", "3", "2", "1", "name")),
                result.getAdditionalNodes());
    }

    @Test
    void compareSchemasDifferentPrimitiveFieldTypeDeeplyNested() {
        ParquetSchemaNode id32 = new ParquetSchemaNode("id", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT32, null);
        ParquetSchemaNode id64 = new ParquetSchemaNode("id", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT64, null);

        int depth = 5;
        ParquetSchemaNode firstSchema = createNestedSchema("root", id32, depth);
        ParquetSchemaNode secondSchema = createNestedSchema("root", id64, depth);

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of(new ParquetSchemaPrimitiveTypeDiff(
                        new ParquetSchemaNodePath("root", "4", "3", "2", "1", "id"),
                        PrimitiveType.PrimitiveTypeName.INT32,
                        PrimitiveType.PrimitiveTypeName.INT64)),
                result.getPrimitiveTypeDiffs());
    }

    @Test
    void findSchemasDifferences() {
        ParquetSchemaNode id = new ParquetSchemaNode("id", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT32, null);
        ParquetSchemaNode name = new ParquetSchemaNode("name", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());
        ParquetSchemaNode email = new ParquetSchemaNode("email", PRIMITIVE, REQUIRED,
                PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType());

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED, null,
                null, List.of(id, name));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", MESSAGE, REPEATED, null,
                null, List.of(id, email));


        ParquetDetails firstParquet = TestUtils.generateParquetDetails(firstSchema);
        ParquetDetails secondParquet = TestUtils.generateParquetDetails(firstSchema);
        ParquetDetails thirdParquet = TestUtils.generateParquetDetails(secondSchema);
        ParquetDetails fourthParquet = TestUtils.generateParquetDetails(secondSchema);

        ParquetSchemaDiff expectedDiff = ParquetComparator.compareSchemas(secondParquet, thirdParquet);
        List<ParquetSchemaDiff> result = ParquetComparator.findSchemasDifferences(List.of(firstParquet, secondParquet,
                thirdParquet, fourthParquet));

        assertEquals(1, result.size());
        assertEquals(expectedDiff, result.get(0));
    }

    private ParquetSchemaNode createNestedSchema(String baseName, ParquetSchemaNode finalPrimitiveNode, int depth) {
        if (depth == 0) {
            return finalPrimitiveNode;
        }

        ParquetSchemaNode child = createNestedSchema(String.valueOf(depth - 1), finalPrimitiveNode, depth - 1);
        return new ParquetSchemaNode(baseName, ParquetSchemaType.GROUP, REQUIRED, null, null, List.of(child));
    }

    private ParquetSchemaDiff compareSchemas(ParquetSchemaNode firstSchema, ParquetSchemaNode secondSchema) {
        return ParquetComparator.compareSchemas(
                TestUtils.generateParquetDetails(firstSchema),
                TestUtils.generateParquetDetails(secondSchema));
    }
}
