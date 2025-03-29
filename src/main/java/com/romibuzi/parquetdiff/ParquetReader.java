package com.romibuzi.parquetdiff;

import com.romibuzi.parquetdiff.models.ParquetDetails;
import com.romibuzi.parquetdiff.models.ParquetSchemaNode;
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
import java.util.List;

public class ParquetReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetReader.class);
    private static final String PARQUET_EXTENSION = ".parquet";
    private static final ParquetReadOptions PARQUET_READ_OPTIONS = ParquetReadOptions.builder().build();

    private final FileSystem fileSystem;
    private final ParquetTypeVisitor typeVisitor;

    public ParquetReader(FileSystem fileSystem) {
        this.fileSystem = fileSystem;
        this.typeVisitor = new ParquetTypeVisitor();
    }

    public List<ParquetDetails> readParquetDirectory(String parquetDirectory) {
        return readAllParquetInDirectory(new Path(parquetDirectory));
    }

    private List<ParquetDetails> readAllParquetInDirectory(Path path) {
        List<ParquetDetails> results = new ArrayList<>();

        for (FileStatus fileStatus : getFileStatuses(path)) {
            if (fileStatus.isDirectory()) {
                results.addAll(readAllParquetInDirectory(fileStatus.getPath()));
            } else if (fileStatus.getPath().getName().endsWith(PARQUET_EXTENSION)) {
                ParquetDetails details = getParquetDetails(fileStatus);
                if (details != null) {
                    results.add(details);
                }
            }
        }

        return results;
    }

    private FileStatus[] getFileStatuses(Path path) {
        try {
            return fileSystem.listStatus(path);
        } catch (IOException e) {
            LOGGER.error("Could not listStatus on {}", path, e);
            return new FileStatus[]{};
        }
    }

    private ParquetDetails getParquetDetails(FileStatus fileStatus) {
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
            return null;
        }
    }

    private ParquetSchemaNode extractSchema(MessageType messageType) {
        messageType.accept(typeVisitor);
        return typeVisitor.getSchema();
    }
}
