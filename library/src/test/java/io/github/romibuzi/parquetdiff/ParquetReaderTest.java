package io.github.romibuzi.parquetdiff;

import io.github.romibuzi.parquetdiff.metadata.ParquetDetails;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class ParquetReaderTest {
    private final String parquetDirectory = Paths.get("src", "test", "resources", "test_data.parquet")
            .toAbsolutePath().toString();

    @Test
    void readParquetDirectory() throws IOException {
        FileSystem fileSystem = FileSystem.get(new Configuration());
        ParquetReader parquetReader = new ParquetReader(fileSystem);

        List<ParquetDetails> results = parquetReader.readParquetDirectory(parquetDirectory);

        assertEquals(2, results.size());

        ParquetDetails firstParquet = results.get(0);
        assertEquals(parquetPartitionPath(parquetDirectory, "date=2020-12-27"), firstParquet.path());
        assertEquals(1, firstParquet.numRows());

        ParquetDetails secondParquet = results.get(1);
        assertEquals(parquetPartitionPath(parquetDirectory, "date=2020-12-28"), secondParquet.path());
        assertEquals(1, secondParquet.numRows());
    }

    @Test
    void readParquetDirectoryNotFound() throws IOException {
        FileSystem fileSystem = mock(FileSystem.class);
        Path parquetDirectoryPath = new Path(parquetDirectory);
        when(fileSystem.exists(eq(parquetDirectoryPath))).thenReturn(false);

        ParquetReader parquetReader = new ParquetReader(fileSystem);

        IOException exception = assertThrows(IOException.class,
                () -> parquetReader.readParquetDirectory(parquetDirectory));

        assertEquals("Parquet directory not found: " + parquetDirectory, exception.getMessage());
        verify(fileSystem).exists(eq(parquetDirectoryPath));
        verifyNoMoreInteractions(fileSystem);
    }

    @Test
    void readParquetDirectoryWithError() throws IOException {
        FileSystem fileSystem = mock(FileSystem.class);
        Path parquetDirectoryPath = new Path(parquetDirectory);
        when(fileSystem.exists(eq(parquetDirectoryPath))).thenReturn(true);
        when(fileSystem.listStatus(eq(parquetDirectoryPath))).thenThrow(IOException.class);

        ParquetReader parquetReader = new ParquetReader(fileSystem);

        assertThrows(IOException.class, () -> parquetReader.readParquetDirectory(parquetDirectory));
        verify(fileSystem).exists(eq(parquetDirectoryPath));
        verify(fileSystem).listStatus(eq(parquetDirectoryPath));
        verifyNoMoreInteractions(fileSystem);
    }

    private Path parquetPartitionPath(String parquetDirectory, String partition) {
        URI uri = Paths.get(parquetDirectory, partition, "part-00000.parquet").toUri();
        return new Path(uri);
    }
}
