package io.github.romibuzi.parquetdiff.metadata;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Represents a path to the given node in a schema.
 */
public final class ParquetSchemaNodePath {
    private final List<String> components;

    /**
     * <p>
     * Instantiate a ParquetSchemaNodePath with the given components list. Example:
     * <pre>{@code
     * ParquetSchemaNodePath nodePath = new ParquetSchemaNodePath(List.of("root", "address", "city"));
     * }</pre>
     *
     * @param components components of the Path
     */
    public ParquetSchemaNodePath(List<String> components) {
        this.components = components;
    }

    /**
     * Instantiate an empty path.
     */
    public ParquetSchemaNodePath() {
        this(Collections.emptyList());
    }

    /**
     * <p>
     * Instantiate a ParquetSchemaNodePath with the given components varargs. Example:
     * <pre>{@code
     * ParquetSchemaNodePath nodePath = new ParquetSchemaNodePath("root", "address", "city");
     * }</pre>
     *
     * @param components components of the Path
     */
    public ParquetSchemaNodePath(String... components) {
        this(Arrays.stream(components).collect(Collectors.toList()));
    }

    /**
     * Adds a new component to the current path.
     * <pre>{@code
     * ParquetSchemaNodePath nodePath = new ParquetSchemaNodePath("root", "address");
     * ParquetSchemaNodePath newNodePath = nodePath.addComponent(""city"");
     * nodePath.getComponents(); // ["root", "address"]
     * newNodePath.getComponents(); // ["root", "address", "city"]
     * }</pre>
     *
     * @param component The new component.
     * @return A new ParquetSchemaNodePath instance with the component appended.
     */
    public ParquetSchemaNodePath add(String component) {
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
     * <p>
     * Export the path as string. Example:
     * <pre>{@code
     * ParquetSchemaNodePath nodePath = new ParquetSchemaNodePath("root", "address", "city");
     * nodePath.toPathString("||"); // root||address|city
     * }</pre>
     *
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
