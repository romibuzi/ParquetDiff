package com.romibuzi.parquetdiff;

import com.romibuzi.parquetdiff.models.ParquetDetails;
import com.romibuzi.parquetdiff.models.ParquetPartitions;
import com.romibuzi.parquetdiff.models.ParquetSchemaDiff;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public final class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private final ParquetReader parquetReader;

    public Main(FileSystem fileSystem) {
        parquetReader = new ParquetReader(fileSystem);
    }

    /**
     * @param parquetDirectory the Parquet directory to analyze.
     */
    public void run(String parquetDirectory) {
        List<ParquetDetails> parquets = readParquetDirectory(parquetDirectory);
        LOGGER.info("Found {} parquets files", parquets.size());

        List<ParquetPartitions> differentPartitionsStructure =
                ParquetCompare.findDifferentPartitionsStructure(parquets);

        if (differentPartitionsStructure.isEmpty()) {
            System.out.println("All Parquet partitions have the same structure");
        }

        List<ParquetSchemaDiff> schemasDifferences = ParquetCompare.findSchemasDifferences(parquets);
        if (schemasDifferences.isEmpty()) {
            System.out.println("All Parquet partitions have the same schema");
        } else {
            schemasDifferences.forEach(ParquetSchemaDiff::printDifferences);
        }
    }

    private List<ParquetDetails> readParquetDirectory(String parquetDirectory) {
        try {
            return parquetReader.readParquetDirectory(parquetDirectory);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        throw new RuntimeException();
    }

    private static FileSystem initFileSystem() {
        try {
            return FileSystem.get(new Configuration());
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        throw new RuntimeException();
    }

    /**
     * @param args CLI arguments.
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: ParquetDiff <parquet-path>");
            System.exit(1);
        }

        Main main = new Main(initFileSystem());
        main.run(args[0]);
    }
}
