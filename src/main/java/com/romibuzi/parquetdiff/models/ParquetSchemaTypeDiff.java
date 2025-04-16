package com.romibuzi.parquetdiff.models;

/**
 * Represents a Type difference. e.g. PRIMITIVE vs GROUP.
 *
 * @param nodePath The path to the node in the schema.
 * @param oldType  The type associated to that node in the previous schema.
 * @param newType  The type associated to that node in the new schema.
 */
public record ParquetSchemaTypeDiff(String nodePath,
                                    ParquetSchemaType oldType,
                                    ParquetSchemaType newType) {
}
