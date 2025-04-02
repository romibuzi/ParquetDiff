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
    void findSchemasDifferencesIdentical() {
        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null);
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null);

        assertFalse(ParquetCompare.findSchemasDifferences(firstSchema, secondSchema).hasDifferences());
    }

    @Test
    void findSchemasDifferencesIdenticalWithFields() {
        ParquetSchemaNode id = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.INT32);
        ParquetSchemaNode name = new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.BINARY);

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id, name));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id, name));

        assertFalse(ParquetCompare.findSchemasDifferences(firstSchema, secondSchema).hasDifferences());
    }

    @Test
    void findSchemasDifferencesAdditionalField() {
        ParquetSchemaNode id = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.INT32);
        ParquetSchemaNode name = new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.BINARY);

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id, name));

        ParquetSchemaDiff result = ParquetCompare.findSchemasDifferences(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of("test_schema.name"), result.additionalNodes());
    }

    @Test
    void findSchemasDifferencesMissingField() {
        ParquetSchemaNode id = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.INT32);
        ParquetSchemaNode name = new ParquetSchemaNode("name", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.BINARY);

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id, name));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id));

        ParquetSchemaDiff result = ParquetCompare.findSchemasDifferences(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of("test_schema.name"), result.missingNodes());
    }

    @Test
    void findSchemasDifferencesDifferentFieldType() {
        ParquetSchemaNode id32 = new ParquetSchemaNode("id", ParquetSchemaType.GROUP, null);
        ParquetSchemaNode id64 = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.INT64);

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id32));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id64));

        ParquetSchemaDiff result = ParquetCompare.findSchemasDifferences(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of(new ParquetSchemaTypeDiff("test_schema.id", ParquetSchemaType.GROUP,
                ParquetSchemaType.PRIMITIVE)), result.typeDiffs());
    }

    @Test
    void findSchemasDifferencesDifferentPrimitiveFieldType() {
        ParquetSchemaNode id32 = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.INT32);
        ParquetSchemaNode id64 = new ParquetSchemaNode("id", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.INT64);

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id32));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(id64));

        ParquetSchemaDiff result = ParquetCompare.findSchemasDifferences(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of(new ParquetSchemaPrimitiveTypeDiff("test_schema.id",
                        PrimitiveType.PrimitiveTypeName.INT32, PrimitiveType.PrimitiveTypeName.INT64)),
                result.primitiveTypeDiffs());
    }

    @Test
    void findSchemasDifferencesAdditionalAndMissingField() {
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

        ParquetSchemaDiff result = ParquetCompare.findSchemasDifferences(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of("test_schema.name"), result.missingNodes());
        assertEquals(List.of("test_schema.email"), result.additionalNodes());
    }

    @Test
    void findSchemasDifferencesAdditionalAndMissingFieldNested() {
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

        ParquetSchemaDiff result = ParquetCompare.findSchemasDifferences(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of("test_schema.address.zip_code"), result.missingNodes());
        assertEquals(List.of("test_schema.address.city"), result.additionalNodes());
    }
}
