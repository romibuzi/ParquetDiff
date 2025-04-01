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

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null);
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null);
        firstSchema.addChild(id);
        firstSchema.addChild(name);
        secondSchema.addChild(id);
        secondSchema.addChild(name);

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
    void findSchemasDifferencesNestedMissingField() {
        ParquetSchemaNode street = new ParquetSchemaNode("street", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.BINARY);
        ParquetSchemaNode zipCode = new ParquetSchemaNode("zip_code", ParquetSchemaType.PRIMITIVE,
                PrimitiveType.PrimitiveTypeName.BINARY);

        ParquetSchemaNode address = new ParquetSchemaNode("address", ParquetSchemaType.GROUP, null,
                List.of(street, zipCode));
        ParquetSchemaNode addressWithoutZipCode = new ParquetSchemaNode("address", ParquetSchemaType.GROUP, null,
                List.of(street));

        ParquetSchemaNode firstSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(address));
        ParquetSchemaNode secondSchema = new ParquetSchemaNode("test_schema", ParquetSchemaType.MESSAGE, null,
                List.of(addressWithoutZipCode));

        ParquetSchemaDiff result = ParquetCompare.findSchemasDifferences(firstSchema, secondSchema);
        assertTrue(result.hasDifferences());
        assertEquals(List.of("test_schema.address.zip_code"), result.missingNodes());
    }
}
