package io.github.romibuzi.parquetdiff;

import io.github.romibuzi.parquetdiff.metadata.ParquetDetails;
import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Provide facilities to read Parquet directories and files.
 */
public final class ParquetReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetReader.class);
    private static final String PARQUET_EXTENSION = ".parquet";
    private static final ParquetReadOptions PARQUET_READ_OPTIONS = ParquetReadOptions.builder().build();

    private final FileSystem fileSystem;
    private final ParquetTypeVisitor typeVisitor = new ParquetTypeVisitor();

    /**
     * <p>
     * Instantiate a ParquetReader instance with the given Hadoop filesystem. Example:
     * <pre>{@code
     * import org.apache.hadoop.conf.Configuration;
     * import org.apache.hadoop.fs.FileSystem;
     *
     * ParquetReader reader = new ParquetReader(FileSystem.get(new Configuration));
     * }</pre>
     *
     * @param fileSystem A configured Hadoop filesystem.
     */
    public ParquetReader(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
    }

    /**
     * @return ParquetReader with a standard Hadoop configuration.
     * @throws IOException If an IO error happened during Hadoop Filesystem initialization.
     */
    public static ParquetReader getDefault() throws IOException {
        return new ParquetReader(FileSystem.get(new Configuration()));
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
        FileStatus fileStatus = fileSystem.getFileStatus(parquetDirectoryPath);
        if (!fileStatus.isDirectory()) {
            throw new IOException("Parquet is not a directory: " + parquetDirectoryPath);
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
        return extractParquetDetails(fileStatus);
    }

    /**
     * @see ParquetReader#readParquetFile(Path)
     */
    public ParquetDetails readParquetFile(String parquetFile) throws IOException {
        return readParquetFile(new Path(parquetFile));
    }

    private List<ParquetDetails> readAllParquetsInDirectory(Path path) throws IOException {
        List<ParquetDetails> results = new ArrayList<>();
        Queue<Path> directoriesToProcess = new ArrayDeque<>();
        directoriesToProcess.offer(path);

        while (!directoriesToProcess.isEmpty()) {
            Path currentDirectory = directoriesToProcess.poll();
            for (FileStatus fileStatus : listFileStatuses(currentDirectory)) {
                if (fileStatus.isDirectory()) {
                    directoriesToProcess.offer(fileStatus.getPath());
                } else if (fileStatus.getPath().getName().endsWith(PARQUET_EXTENSION)) {
                    ParquetDetails details = extractParquetDetails(fileStatus);
                    results.add(details);
                }
            }
        }

        return results;
    }

    private ParquetDetails extractParquetDetails(FileStatus fileStatus) throws IOException {
        try {
            ParquetMetadata metadata = readParquetFooter(fileStatus);
            return new ParquetDetails(
                    fileStatus.getPath(),
                    extractRowCount(metadata.getBlocks()),
                    extractSchema(metadata.getFileMetaData().getSchema()));
        } catch (IOException e) {
            LOGGER.error("Error reading Parquet footer: {}", fileStatus.getPath(), e);
            throw e;
        }
    }

    private long extractRowCount(List<BlockMetaData> blocks) {
        return blocks.stream().mapToLong(BlockMetaData::getRowCount).sum();
    }

    private ParquetSchemaNode extractSchema(MessageType messageType) {
        messageType.accept(typeVisitor);
        return typeVisitor.getSchema();
    }

    private FileStatus[] listFileStatuses(Path path) throws IOException {
        try {
            FileStatus[] fileStatuses = fileSystem.listStatus(path);
            Arrays.sort(fileStatuses, Comparator.comparing(FileStatus::getPath));
            return fileStatuses;
        } catch (IOException e) {
            LOGGER.error("Could not listStatus on {}", path, e);
            throw e;
        }
    }

    private ParquetMetadata readParquetFooter(FileStatus fileStatus) throws IOException {
        InputFile inputFile = HadoopInputFile.fromStatus(fileStatus, fileSystem.getConf());
        try (SeekableInputStream stream = inputFile.newStream()) {
            return ParquetFileReader.readFooter(inputFile, PARQUET_READ_OPTIONS, stream);
        }
    }
}
