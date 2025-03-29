package com.romibuzi.parquetdiff;

import com.romibuzi.parquetdiff.models.ParquetDetails;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class ParquetReaderTest {
    private final java.nio.file.Path resourceDirectory = Paths.get("src", "test", "resources");
    private final java.nio.file.Path parquetDirectory = resourceDirectory.resolve("test_data.parquet").toAbsolutePath();

    @Test
    void readParquetDirectory() throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        ParquetReader parquetReader = new ParquetReader(fileSystem);

        List<ParquetDetails> results = parquetReader.readParquetDirectory(parquetDirectory.toString());

        assertEquals(2, results.size());

        ParquetDetails firstParquet = results.get(0);
        assertEquals(parquetPartitionPath(parquetDirectory, "date=2020-12-27"), firstParquet.path());
        assertEquals(1, firstParquet.numRows());

        ParquetDetails secondParquet = results.get(1);
        assertEquals(parquetPartitionPath(parquetDirectory, "date=2020-12-28"), secondParquet.path());
        assertEquals(1, secondParquet.numRows());
    }

    @Test
    void readParquetDirectoryWithError() throws IOException {
        FileSystem fileSystem = mock(FileSystem.class);
        when(fileSystem.listStatus(eq(new Path(parquetDirectory.toString())))).thenThrow(IOException.class);

        ParquetReader parquetReader = new ParquetReader(fileSystem);
        List<ParquetDetails> results = parquetReader.readParquetDirectory(parquetDirectory.toString());

        assertTrue(results.isEmpty());
        verify(fileSystem).listStatus(eq(new Path(parquetDirectory.toString())));
        verifyNoMoreInteractions(fileSystem);
    }

    private Path parquetPartitionPath(java.nio.file.Path parquetDirectory, String partition) {
        URI uri = parquetDirectory.resolve(Paths.get(partition, "part-00000.parquet")).toUri();
        return new Path(uri);
    }
}
