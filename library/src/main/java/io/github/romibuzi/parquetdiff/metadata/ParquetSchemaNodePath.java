package io.github.romibuzi.parquetdiff.metadata;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Represents a path to the given node in a schema.
 */
public final class ParquetSchemaNodePath {
    private final List<String> components;

    public ParquetSchemaNodePath(List<String> components) {
        this.components = components;
    }

    public ParquetSchemaNodePath() {
        this(Collections.emptyList());
    }

    public ParquetSchemaNodePath(String... segments) {
        this(Arrays.stream(segments).collect(Collectors.toList()));
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
     * @return components of the path.
     */
    public List<String> getComponents() {
        return components;
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

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParquetSchemaNodePath that = (ParquetSchemaNodePath) o;
        return Objects.equals(components, that.components);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(components);
    }
}
