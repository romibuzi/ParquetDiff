package com.romibuzi.parquetdiff;

import com.romibuzi.parquetdiff.models.*;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ParquetCompareTest {
    @Test
    void findDifferentPartitionsStructureEmpty() {
        List<ParquetPartitions> results = ParquetCompare.findDifferentPartitionsStructure(Collections.emptyList());
        assertTrue(results.isEmpty());
    }

    @Test
    void findDifferentPartitionsStructureSinglePartition() {
        String path = "vaccinations.parquet/date=2020-12-28/country=Spain/part-0000.parquet";
        ParquetDetails first = new ParquetDetails(new Path(path), 10, null);
        List<ParquetPartitions> results = ParquetCompare.findDifferentPartitionsStructure(List.of(first));
        assertTrue(results.isEmpty());
    }

    @Test
    void findDifferentPartitionsStructureAllSame() {
        String firstPath = "vaccinations.parquet/date=2020-12-28/country=Spain/part-0000.parquet";
        String secondPath = "vaccinations.parquet/date=2020-12-29/country=Spain/part-0000.parquet";
        ParquetDetails first = new ParquetDetails(new Path(firstPath), 10, null);
        ParquetDetails second = new ParquetDetails(new Path(secondPath), 10, null);
        List<ParquetPartitions> results = ParquetCompare.findDifferentPartitionsStructure(List.of(first, second));
        assertTrue(results.isEmpty());
    }

    @Test
    void findDifferentPartitionsStructure() {
        String firstPath = "vaccinations.parquet/date=2020-12-28/country=Spain/part-0000.parquet";
        String secondPath = "vaccinations.parquet/date=2020-12-29/part-0000.parquet";
        ParquetDetails first = new ParquetDetails(new Path(firstPath), 10, null);
        ParquetDetails second = new ParquetDetails(new Path(secondPath), 10, null);
        List<ParquetPartitions> results = ParquetCompare.findDifferentPartitionsStructure(List.of(first, second));
        assertEquals(2, results.size());
        assertEquals(List.of("date", "country"), results.get(0).keys());
        assertEquals(List.of("date"), results.get(1).keys());
    }

    @Test
    void compareSchemasIdentical() {
        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null);
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null);

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertFalse(result.hasDifferences());
    }

    @Test
    void compareSchemasIdenticalWithFields() {
        ParquetSchemaNode id = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.INT32);
        ParquetSchemaNode name = new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.BINARY);

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id, name));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id, name));

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertFalse(result.hasDifferences());
    }

    @Test
    void compareSchemasAdditionalField() {
        ParquetSchemaNode id = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.INT32);
        ParquetSchemaNode name = new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.BINARY);

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id, name));

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of("test_schema.name"), result.additionalNodes());
    }

    @Test
    void compareSchemasMissingField() {
        ParquetSchemaNode id = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.INT32);
        ParquetSchemaNode name = new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.BINARY);

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id, name));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id));

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of("test_schema.name"), result.missingNodes());
    }

    @Test
    void compareSchemasDifferentFieldType() {
        ParquetSchemaNode id32 = new ParquetSchemaNode("id", ParquetSchemaType.GROUP, null);
        ParquetSchemaNode id64 = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.INT64);

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id32));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id64));

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of(new ParquetSchemaTypeDiff("test_schema.id", ParquetSchemaType.GROUP,
                ParquetSchemaType.PRIMITIVE)), result.typeDiffs());
    }

    @Test
    void compareSchemasDifferentPrimitiveFieldType() {
        ParquetSchemaNode id32 = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.INT32);
        ParquetSchemaNode id64 = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.INT64);

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id32));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id64));

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of(new ParquetSchemaPrimitiveTypeDiff("test_schema.id",
                        PrimitiveType.PrimitiveTypeName.INT32, PrimitiveType.PrimitiveTypeName.INT64)),
                result.primitiveTypeDiffs());
    }

    @Test
    void compareSchemasAdditionalAndMissingField() {
        ParquetSchemaNode id = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.INT32);
        ParquetSchemaNode name = new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.BINARY);
        ParquetSchemaNode email = new ParquetSchemaNode("email", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.BINARY);

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id, name));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id, email));

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of("test_schema.name"), result.missingNodes());
        assertEquals(List.of("test_schema.email"), result.additionalNodes());
    }

    @Test
    void compareSchemasAdditionalAndMissingFieldNested() {
        ParquetSchemaNode street = new ParquetSchemaNode("street", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.BINARY);
        ParquetSchemaNode zipCode = new ParquetSchemaNode("zip_code", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.BINARY);
        ParquetSchemaNode city = new ParquetSchemaNode("city", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.BINARY);

        ParquetSchemaNode firstAddress = new ParquetSchemaNode("address", ParquetSchemaType.GROUP, null,
                List.of(street, zipCode));
        ParquetSchemaNode secondAddress = new ParquetSchemaNode("address", ParquetSchemaType.GROUP, null,
                List.of(street, city));

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(firstAddress));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(secondAddress));

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of("test_schema.address.zip_code"), result.missingNodes());
        assertEquals(List.of("test_schema.address.city"), result.additionalNodes());
    }

    @Test
    void compareSchemasAdditionalAndMissingFieldDeeplyNested() {
        ParquetSchemaNode id32 = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.INT32);
        ParquetSchemaNode name = new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.BINARY);

        int depth = 5;
        ParquetSchemaNode firstSchema = createNestedSchema("root", id32, depth);
        ParquetSchemaNode secondSchema = createNestedSchema("root", name, depth);

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of("root.4.3.2.1.id"), result.missingNodes());
        assertEquals(List.of("root.4.3.2.1.name"), result.additionalNodes());
    }

    @Test
    void compareSchemasDifferentPrimitiveFieldTypeDeeplyNested() {
        ParquetSchemaNode id32 = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.INT32);
        ParquetSchemaNode id64 = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.INT64);

        int depth = 5;
        ParquetSchemaNode firstSchema = createNestedSchema("root", id32, depth);
        ParquetSchemaNode secondSchema = createNestedSchema("root", id64, depth);

        ParquetSchemaDiff result = compareSchemas(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of(new ParquetSchemaPrimitiveTypeDiff("root.4.3.2.1.id",
                        PrimitiveType.PrimitiveTypeName.INT32, PrimitiveType.PrimitiveTypeName.INT64)),
                result.primitiveTypeDiffs());
    }

    private ParquetSchemaNode createNestedSchema(String baseName, ParquetSchemaNode finalPrimitiveNode, int depth) {
        if (depth == 0) {
            return finalPrimitiveNode;
        }

        ParquetSchemaNode child = createNestedSchema(String.valueOf(depth - 1), finalPrimitiveNode, depth - 1);
        return new ParquetSchemaNode(baseName, ParquetSchemaType.GROUP, null, List.of(child));
    }

    private ParquetSchemaDiff compareSchemas(ParquetSchemaNode firstSchema, ParquetSchemaNode secondSchema) {
        return ParquetCompare.compareSchemas(generateParquetDetails(firstSchema), generateParquetDetails(secondSchema));
    }

    private ParquetDetails generateParquetDetails(ParquetSchemaNode schema) {
        return new ParquetDetails(new Path("test_data.parquet"), 1, schema);
    }
}
