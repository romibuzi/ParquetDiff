package com.romibuzi.parquetdiff.models;

public enum ParquetSchemaType {
    /**
     * Root element of a Parquet schema.
     * A MESSAGE encapsulates the entire schema structure.
     */
    MESSAGE,
    /**
     * Field that contains child elements that can either be GROUPS or PRIMITIVES.
     * GROUP fields are containers for nested fields within the schema.
     * It is referred as STRUCT in other systems, such as Spark or Hive.
     */
    GROUP,
    /**
     * Specific GROUP type that represents a list of elements of the same type.
     */
    LIST,
    /**
     * Specific GROUP type that represents a map of key value elements.
     */
    MAP,
    /**
     * Primitive field in the schema.
     * PRIMITIVE fields do not have child elements.
     */
    PRIMITIVE
}
