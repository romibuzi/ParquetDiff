package io.github.romibuzi.parquetdiff;

import io.github.romibuzi.parquetdiff.metadata.ParquetDetails;
import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaNode;
import org.apache.hadoop.fs.Path;

public class TestUtils {
    public static ParquetDetails generateParquetDetails(ParquetSchemaNode schema) {
        return new ParquetDetails(new Path("test_data.parquet/date=2025-04-20/part-000.parquet"), 1, schema);
    }

    public static String[] getLines(String output) {
        return output.split(System.lineSeparator());
    }
}
