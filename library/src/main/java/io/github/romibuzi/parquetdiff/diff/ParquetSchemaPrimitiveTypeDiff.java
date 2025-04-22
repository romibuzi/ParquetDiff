package io.github.romibuzi.parquetdiff.diff;

import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaNodePath;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Objects;

/**
 * Represents a Primitive Type difference, ex: INT32 vs FLOAT.
 */
public final class ParquetSchemaPrimitiveTypeDiff {
    private final ParquetSchemaNodePath nodePath;
    private final PrimitiveType.PrimitiveTypeName oldType;
    private final PrimitiveType.PrimitiveTypeName newType;

    /**
     * @param nodePath The path to the node in the schema.
     * @param oldType  The primitive type associated to that node in the previous schema.
     * @param newType  The primitive type associated to that node in the new schema.
     */
    public ParquetSchemaPrimitiveTypeDiff(ParquetSchemaNodePath nodePath,
                                          PrimitiveType.PrimitiveTypeName oldType,
                                          PrimitiveType.PrimitiveTypeName newType) {
        this.nodePath = nodePath;
        this.oldType = oldType;
        this.newType = newType;
    }

    public ParquetSchemaNodePath getNodePath() {
        return nodePath;
    }

    public PrimitiveType.PrimitiveTypeName getOldType() {
        return oldType;
    }

    public PrimitiveType.PrimitiveTypeName getNewType() {
        return newType;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParquetSchemaPrimitiveTypeDiff that = (ParquetSchemaPrimitiveTypeDiff) o;
        return Objects.equals(nodePath, that.nodePath) && oldType == that.oldType && newType == that.newType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodePath, oldType, newType);
    }
}
