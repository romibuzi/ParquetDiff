package com.romibuzi.parquetdiff.diff;

import org.apache.parquet.schema.PrimitiveType;

/**
 * Represents a Primitive Type difference. e.g. INT32 vs FLOAT.
 *
 * @param nodePath The path to the node in the schema.
 * @param oldType  The primitive type associated to that node in the previous schema.
 * @param newType  The primitive type associated to that node in the new schema.
 */
public record ParquetSchemaPrimitiveTypeDiff(String nodePath,
                                             PrimitiveType.PrimitiveTypeName oldType,
                                             PrimitiveType.PrimitiveTypeName newType) {
}
