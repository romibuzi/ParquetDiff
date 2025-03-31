package com.romibuzi.parquetdiff.models;

public enum ParquetSchemaType {
    /**
     * The root element of a Parquet schema.
     * A MESSAGE encapsulates the entire schema structure.
     */
    MESSAGE,
    /**
     * Field that contains child elements.
     * GROUP fields are containers for nested fields within the schema.
     */
    GROUP,
    /**
     * Primitive field in the schema.
     * PRIMITIVE fields do not have child elements.
     */
    PRIMITIVE
}
