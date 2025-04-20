package io.github.romibuzi.parquetdiff.diff;

import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaNodePath;
import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaType;

/**
 * Represents a Type difference, ex: GROUP vs PRIMITIVE.
 *
 * @param nodePath The path to the node in the schema.
 * @param oldType  The type associated to that node in the previous schema.
 * @param newType  The type associated to that node in the new schema.
 */
public record ParquetSchemaTypeDiff(ParquetSchemaNodePath nodePath,
                                    ParquetSchemaType oldType,
                                    ParquetSchemaType newType) {
}
