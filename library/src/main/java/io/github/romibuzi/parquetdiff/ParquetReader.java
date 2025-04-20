package io.github.romibuzi.parquetdiff;

import io.github.romibuzi.parquetdiff.metadata.ParquetDetails;
import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaNode;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Provide facilities to read Parquet directory and files.
 */
public final class ParquetReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetReader.class);
    private static final String PARQUET_EXTENSION = ".parquet";
    private static final ParquetReadOptions PARQUET_READ_OPTIONS = ParquetReadOptions.builder().build();

    private final FileSystem fileSystem;
    private final ParquetTypeVisitor typeVisitor = new ParquetTypeVisitor();

    /**
     * @param fileSystem A configured Hadoop filesystem.
     */
    public ParquetReader(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    /**
     * Extract partitions and files metadata from a Parquet directory.
     *
     * @param parquetDirectoryPath the Parquet directory to read.
     * @return All Parquet files extracted as {@link ParquetDetails}.
     * @throws IOException If Parquet directory is not found or if any error happens while listing Parquet files.
     */
    public List<ParquetDetails> readParquetDirectory(Path parquetDirectoryPath) throws IOException {
        if (!fileSystem.exists(parquetDirectoryPath)) {
            throw new IOException("Parquet directory not found: " + parquetDirectoryPath);
        }
        return readAllParquetsInDirectory(parquetDirectoryPath);
    }

    /**
     * @see ParquetReader#readParquetDirectory(Path)
     */
    public List<ParquetDetails> readParquetDirectory(String parquetDirectory) throws IOException {
        return readParquetDirectory(new Path(parquetDirectory));
    }

    /**
     * Extract partitions and metadata from a single Parquet file.
     *
     * @param parquetFilePath the Parquet file to read.
     * @return Parquet file extracted as {@link ParquetDetails}.
     * @throws IOException If Parquet file is not found or if any error happens while reading it.
     */
    public ParquetDetails readParquetFile(Path parquetFilePath) throws IOException {
        if (!fileSystem.exists(parquetFilePath)) {
            throw new IOException("Parquet file not found: " + parquetFilePath);
        }
        FileStatus fileStatus = fileSystem.getFileStatus(parquetFilePath);
        if (!fileStatus.isFile()) {
            throw new IOException("Parquet is not a file: " + parquetFilePath);
        }
        return getParquetDetails(fileStatus);
    }

    /**
     * @see ParquetReader#readParquetFile(Path)
     */
    public ParquetDetails readParquetFile(String parquetFile) throws IOException {
        return readParquetFile(new Path(parquetFile));
    }

    private List<ParquetDetails> readAllParquetsInDirectory(Path path) throws IOException {
        List<ParquetDetails> results = new ArrayList<>();

        for (FileStatus fileStatus : getFileStatuses(path)) {
            if (fileStatus.isDirectory()) {
                results.addAll(readAllParquetsInDirectory(fileStatus.getPath()));
            } else if (fileStatus.getPath().getName().endsWith(PARQUET_EXTENSION)) {
                ParquetDetails details = getParquetDetails(fileStatus);
                results.add(details);
            }
        }

        return results;
    }

    private FileStatus[] getFileStatuses(Path path) throws IOException {
        try {
            FileStatus[] fileStatuses = fileSystem.listStatus(path);
            Arrays.sort(fileStatuses, Comparator.comparing(FileStatus::getPath));
            return fileStatuses;
        } catch (IOException e) {
            LOGGER.error("Could not listStatus on {}", path, e);
            throw e;
        }
    }

    private ParquetDetails getParquetDetails(FileStatus fileStatus) throws IOException {
        try {
            HadoopInputFile inputFile = HadoopInputFile.fromStatus(fileStatus, fileSystem.getConf());
            try (ParquetFileReader metadata = ParquetFileReader.open(inputFile, PARQUET_READ_OPTIONS)) {
                return new ParquetDetails(
                        fileStatus.getPath(),
                        metadata.getRecordCount(),
                        extractSchema(metadata.getFileMetaData().getSchema())
                );
            }
        } catch (IOException e) {
            LOGGER.error("Error reading Parquet file: {}", fileStatus.getPath(), e);
            throw e;
        }
    }

    private ParquetSchemaNode extractSchema(MessageType messageType) {
        messageType.accept(typeVisitor);
        return typeVisitor.getSchema();
    }
}
