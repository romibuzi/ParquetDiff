package com.romibuzi.parquetdiff;

import com.romibuzi.parquetdiff.models.ParquetDetails;
import com.romibuzi.parquetdiff.models.ParquetPartitions;
import com.romibuzi.parquetdiff.models.ParquetSchemaNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Usage: ParquetDiff <parquet-path>");
            System.exit(1);
        }
        String parquetPath = args[0];

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);

        ParquetReader parquetReader = new ParquetReader(fileSystem);
        List<ParquetDetails> parquets = parquetReader.readParquetDirectory(parquetPath);

        List<ParquetPartitions> differentPartitionsStructure = ParquetCompare.findDifferentPartitionsStructure(parquets);

        if (differentPartitionsStructure.isEmpty()) {
            System.out.println("All Parquet partitions have the same structure");
        }

        ParquetDetails parquetDetails = parquets.get(0);
        ParquetTypeVisitor parquetTypeVisitor = new ParquetTypeVisitor();
        MessageType messageType = extractSchema(parquetDetails.path().toString());
        if (messageType != null) {
            messageType.accept(parquetTypeVisitor);
            ParquetSchemaNode schema = parquetTypeVisitor.getSchema();
        }
    }

    private static MessageType extractSchema(String filePath) {
        Configuration configuration = new Configuration();
        Path path = new Path(filePath);
        ParquetReadOptions readOptions = ParquetReadOptions.builder().build();
        try {
            HadoopInputFile inputFile = HadoopInputFile.fromPath(path, configuration);

            try (ParquetFileReader metadata = ParquetFileReader.open(inputFile, readOptions)) {
                return metadata.getFileMetaData().getSchema();
            }
        } catch (Exception e) {
            LOGGER.error("Error reading Parquet file: {}", filePath, e);
            return null;
        }
    }
}
