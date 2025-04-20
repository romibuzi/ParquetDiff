package io.github.romibuzi.parquetdiff.metadata;

import java.util.Locale;

public enum ParquetSchemaType {
    /**
     * Root element of a Parquet schema.
     * A MESSAGE encapsulates the entire schema structure.
     */
    MESSAGE {
        @Override
        public String toString() {
            return "root";
        }
    },
    /**
     * Field that contains child elements that can either be GROUPS or PRIMITIVES.
     * GROUP fields are containers for nested fields within the schema.
     * It is referred as STRUCT in other systems, such as Spark or Hive.
     */
    GROUP {
        @Override
        public String toString() {
            return "struct";
        }
    },
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
    PRIMITIVE;

    @Override
    public String toString() {
        return super.toString().toLowerCase(Locale.ROOT);
    }
}
