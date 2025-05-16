package io.github.romibuzi.parquetdiff;

import io.github.romibuzi.parquetdiff.metadata.ParquetDetails;
import io.github.romibuzi.parquetdiff.metadata.ParquetPartition;
import io.github.romibuzi.parquetdiff.metadata.ParquetPartitions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class ParquetReaderTest {
    private static final String DIRECTORY =
            Paths.get("src", "test", "resources", "test_data.parquet").toAbsolutePath().toString();
    private static final Path DIRECTORY_PATH = new Path(DIRECTORY);

    private static FileSystem fileSystem;

    @BeforeAll
    static void setup() throws IOException {
        fileSystem = FileSystem.get(new Configuration());
    }

    @Test
    void readParquetDirectory() throws IOException {
        ParquetReader parquetReader = new ParquetReader(fileSystem);

        List<ParquetDetails> results = parquetReader.readParquetDirectory(DIRECTORY);

        assertEquals(2, results.size());

        ParquetDetails firstParquet = results.get(0);
        assertEquals(parquetPartitionPath("date=2020-12-27"), firstParquet.getPath());
        assertEquals(1, firstParquet.getNumRows());
        assertEquals(new ParquetPartitions(List.of(new ParquetPartition("date", "2020-12-27"))),
                firstParquet.getPartitions());

        ParquetDetails secondParquet = results.get(1);
        assertEquals(parquetPartitionPath("date=2020-12-28"), secondParquet.getPath());
        assertEquals(1, secondParquet.getNumRows());
        assertEquals(new ParquetPartitions(List.of(new ParquetPartition("date", "2020-12-28"))),
                secondParquet.getPartitions());
    }

    @Test
    void readEmptyParquetDirectory() throws IOException {
        String emptyDirectory = Files.createTempDirectory("empty.parquet").toAbsolutePath().toString();
        ParquetReader parquetReader = new ParquetReader(fileSystem);
        List<ParquetDetails> results = parquetReader.readParquetDirectory(emptyDirectory);
        assertTrue(results.isEmpty());
    }

    @Test
    void readParquetDirectoryNotADirectory() {
        ParquetReader parquetReader = new ParquetReader(fileSystem);
        Path filePath = parquetPartitionPath("date=2020-12-27");
        IOException exception = assertThrows(IOException.class, () -> parquetReader.readParquetDirectory(filePath));
        assertEquals("Parquet is not a directory: " + filePath, exception.getMessage());
    }

    @Test
    void readParquetDirectoryNotFound() throws IOException {
        FileSystem fileSystem = mock(FileSystem.class);
        when(fileSystem.exists(eq(DIRECTORY_PATH))).thenReturn(false);
        ParquetReader parquetReader = new ParquetReader(fileSystem);

        IOException exception = assertThrows(IOException.class, () -> parquetReader.readParquetDirectory(DIRECTORY));

        assertEquals("Parquet directory not found: " + DIRECTORY, exception.getMessage());
        verify(fileSystem).exists(eq(DIRECTORY_PATH));
        verifyNoMoreInteractions(fileSystem);
    }

    @Test
    void readParquetDirectoryWithError() throws IOException {
        FileStatus fileStatus = mock(FileStatus.class);
        when(fileStatus.isDirectory()).thenReturn(true);

        FileSystem fileSystem = mock(FileSystem.class);
        when(fileSystem.exists(eq(DIRECTORY_PATH))).thenReturn(true);
        when(fileSystem.getFileStatus(eq(DIRECTORY_PATH))).thenReturn(fileStatus);
        when(fileSystem.listStatus(eq(DIRECTORY_PATH))).thenThrow(IOException.class);
        ParquetReader parquetReader = new ParquetReader(fileSystem);

        assertThrows(IOException.class, () -> parquetReader.readParquetDirectory(DIRECTORY));

        verify(fileSystem).exists(eq(DIRECTORY_PATH));
        verify(fileSystem).getFileStatus(eq(DIRECTORY_PATH));
        verify(fileSystem).listStatus(eq(DIRECTORY_PATH));
        verifyNoMoreInteractions(fileSystem);
    }

    @Test
    void readParquetFile() throws IOException {
        Path parquetFilePath = parquetPartitionPath("date=2020-12-27");
        ParquetReader parquetReader = new ParquetReader(fileSystem);

        ParquetDetails results = parquetReader.readParquetFile(parquetFilePath.toString());

        assertEquals(parquetFilePath, results.getPath());
        assertEquals(1, results.getNumRows());
        assertEquals(new ParquetPartitions(List.of(new ParquetPartition("date", "2020-12-27"))),
                results.getPartitions());
    }

    @Test
    void readParquetFileNotAFile() {
        ParquetReader parquetReader = new ParquetReader(fileSystem);
        IOException exception = assertThrows(IOException.class, () -> parquetReader.readParquetFile(DIRECTORY));
        assertEquals("Parquet is not a file: " + DIRECTORY, exception.getMessage());
    }

    private Path parquetPartitionPath(String partition) {
        URI uri = Paths.get(DIRECTORY, partition, "part-00000.parquet").toUri();
        return new Path(uri);
    }
}
