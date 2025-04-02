package com.romibuzi.parquetdiff.models;

public enum ParquetSchemaType {
    /**
     * Root element of a Parquet schema.
     * A MESSAGE encapsulates the entire schema structure.
     */
    MESSAGE,
    /**
     * Field that contains child elements.
     * GROUP fields are containers for nested fields within the schema.
     * TODO handle logical groups, such as STRUCTS or MAPS
     */
    GROUP,
    /**
     * Primitive field in the schema.
     * PRIMITIVE fields do not have child elements.
     */
    PRIMITIVE
}
