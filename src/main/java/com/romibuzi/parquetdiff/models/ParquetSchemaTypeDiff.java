package com.romibuzi.parquetdiff.models;

public record ParquetSchemaTypeDiff(String nodePath,
                                    ParquetSchemaType oldType,
                                    ParquetSchemaType newType) {
}
