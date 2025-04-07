package com.romibuzi.parquetdiff;

import com.romibuzi.parquetdiff.models.ParquetDetails;
import com.romibuzi.parquetdiff.models.ParquetSchemaNode;
import org.apache.hadoop.fs.Path;

import java.util.function.Supplier;

public class TestUtils {
    public static ParquetDetails generateParquetDetails(ParquetSchemaNode schema) {
        return new ParquetDetails(new Path("test_data.parquet/date=2025-04-20/part-000.parquet"), 1, schema);
    }

    public static String[] getLines(Supplier<String> output) {
        return getLines(output.get());
    }

    private static String[] getLines(String output) {
        return output.split(System.lineSeparator());
    }
}
