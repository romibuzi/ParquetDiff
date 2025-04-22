package io.github.romibuzi.parquetdiff.diff;

import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaNodePath;
import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaType;

import java.util.Objects;

/**
 * Represents a Type difference, ex: GROUP vs PRIMITIVE.
 */
public final class ParquetSchemaTypeDiff {
    private final ParquetSchemaNodePath nodePath;
    private final ParquetSchemaType oldType;
    private final ParquetSchemaType newType;

    /**
     * @param nodePath The path to the node in the schema.
     * @param oldType  The type associated to that node in the previous schema.
     * @param newType  The type associated to that node in the new schema.
     */
    public ParquetSchemaTypeDiff(ParquetSchemaNodePath nodePath,
                                 ParquetSchemaType oldType,
                                 ParquetSchemaType newType) {
        this.nodePath = nodePath;
        this.oldType = oldType;
        this.newType = newType;
    }

    public ParquetSchemaNodePath getNodePath() {
        return nodePath;
    }

    public ParquetSchemaType getOldType() {
        return oldType;
    }

    public ParquetSchemaType getNewType() {
        return newType;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParquetSchemaTypeDiff that = (ParquetSchemaTypeDiff) o;
        return Objects.equals(nodePath, that.nodePath) && oldType == that.oldType && newType == that.newType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodePath, oldType, newType);
    }
}
