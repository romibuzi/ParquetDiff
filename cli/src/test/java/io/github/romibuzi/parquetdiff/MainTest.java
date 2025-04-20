package io.github.romibuzi.parquetdiff;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class MainTest {
    @Test
    void runErrorReadParquetDirectory() throws IOException {
        String parquetDirectory = "/path/to/nonexistent_data.parquet";
        ParquetReader parquetReader = mock(ParquetReader.class);
        when(parquetReader.readParquetDirectory(parquetDirectory)).thenThrow(IOException.class);
        Main main = new Main(parquetReader);

        assertThrows(IOException.class, () -> main.run(parquetDirectory));
        verify(parquetReader).readParquetDirectory(eq(parquetDirectory));
        verifyNoMoreInteractions(parquetReader);
    }
}
