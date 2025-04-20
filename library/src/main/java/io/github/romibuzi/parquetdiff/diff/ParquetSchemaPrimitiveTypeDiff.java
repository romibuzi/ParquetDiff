package io.github.romibuzi.parquetdiff.diff;

import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaNodePath;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Represents a Primitive Type difference, ex: INT32 vs FLOAT.
 *
 * @param nodePath The path to the node in the schema.
 * @param oldType  The primitive type associated to that node in the previous schema.
 * @param newType  The primitive type associated to that node in the new schema.
 */
public record ParquetSchemaPrimitiveTypeDiff(ParquetSchemaNodePath nodePath,
                                             PrimitiveType.PrimitiveTypeName oldType,
                                             PrimitiveType.PrimitiveTypeName newType) {
}
