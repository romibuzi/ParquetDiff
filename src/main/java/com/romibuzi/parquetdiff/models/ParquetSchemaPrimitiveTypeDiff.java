package com.romibuzi.parquetdiff.models;

import org.apache.parquet.schema.PrimitiveType;

public record ParquetSchemaPrimitiveTypeDiff(String nodePath,
                                             PrimitiveType.PrimitiveTypeName oldType,
                                             PrimitiveType.PrimitiveTypeName newType) {
}
