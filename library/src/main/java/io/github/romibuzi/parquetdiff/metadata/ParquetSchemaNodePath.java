package io.github.romibuzi.parquetdiff.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Represents a path to the given node in a schema.
 *
 * @param components components of the path.
 */
public record ParquetSchemaNodePath(List<String> components) {
    public ParquetSchemaNodePath() {
        this(Collections.emptyList());
    }

    public ParquetSchemaNodePath(String... segments) {
        this(Arrays.stream(segments).toList());
    }

    /**
     * Adds a new component to the current path.
     *
     * @param component The new component.
     * @return a new ParquetSchemaNodePath instance with the component appended.
     */
    public ParquetSchemaNodePath addComponent(String component) {
        ArrayList<String> newSegments = new ArrayList<>(this.components);
        newSegments.add(component);
        return new ParquetSchemaNodePath(Collections.unmodifiableList(newSegments));
    }

    /**
     * @param delimiter The delimiter to use between components.
     * @return The string representation of the path.
     */
    public String toPathString(String delimiter) {
        return String.join(delimiter, components);
    }

    @Override
    public String toString() {
        return toPathString(".");
    }
}
