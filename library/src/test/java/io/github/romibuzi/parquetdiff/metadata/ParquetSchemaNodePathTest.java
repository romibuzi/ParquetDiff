package io.github.romibuzi.parquetdiff.metadata;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ParquetSchemaNodePathTest {
    @Test
    void addComponent() {
        ParquetSchemaNodePath emptyPath = new ParquetSchemaNodePath();

        ParquetSchemaNodePath rootPath = emptyPath.addComponent("root");
        assertNotEquals(emptyPath, rootPath);
        assertEquals(List.of("root"), rootPath.getComponents());

        ParquetSchemaNodePath streetPath = rootPath.addComponent("street");
        assertNotEquals(rootPath, streetPath);
        assertEquals(List.of("root", "street"), streetPath.getComponents());
    }

    @Test
    void toPathString() {
        ParquetSchemaNodePath nodePath = new ParquetSchemaNodePath("root", "address", "street");
        assertEquals("root_address_street", nodePath.toPathString("_"));
    }

    @Test
    void toPathStringEmpty() {
        ParquetSchemaNodePath emptyPath = new ParquetSchemaNodePath();
        assertEquals("", emptyPath.toPathString("."));
    }
}