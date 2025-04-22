package io.github.romibuzi.parquetdiff;

import io.github.romibuzi.parquetdiff.diff.ParquetComparator;
import io.github.romibuzi.parquetdiff.diff.ParquetSchemaDiff;
import io.github.romibuzi.parquetdiff.metadata.ParquetDetails;
import io.github.romibuzi.parquetdiff.metadata.ParquetPartitions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public final class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    private static final String UNICODE_GREEN_CROSS = toUnicodeString(0x2705);
    private static final String UNICODE_RED_CROSS = toUnicodeString(0x274C);
    private static final String UNICODE_LARGE_YELLOW_SQUARE = toUnicodeString(0x1F7E8);

    private final ParquetReader parquetReader;

    public Main(ParquetReader parquetReader) {
        this.parquetReader = parquetReader;
    }

    /**
     * @param parquetDirectory the Parquet directory to analyze.
     */
    public void run(String parquetDirectory) throws IOException {
        List<ParquetDetails> parquets = parquetReader.readParquetDirectory(parquetDirectory);
        if (parquets.isEmpty()) {
            LOGGER.info("No parquets files found");
            return;
        }

        LOGGER.info("Found {} partitions and {} parquets files", countNumberOfPartitions(parquets), parquets.size());
        LOGGER.info("Total rows: {}", countNumberOfRows(parquets));

        List<ParquetPartitions> partitionsDifferences = ParquetComparator.findDifferentPartitionsStructure(parquets);
        if (partitionsDifferences.isEmpty()) {
            System.out.println(UNICODE_GREEN_CROSS + " All Parquet partitions have the same structure.");
        } else {
            System.out.println(UNICODE_RED_CROSS + " Conflicting partition structures found.");
            partitionsDifferences.forEach(System.out::println);
        }

        List<ParquetSchemaDiff> schemasDifferences = ParquetComparator.findSchemasDifferences(parquets);
        if (schemasDifferences.isEmpty()) {
            System.out.println(UNICODE_GREEN_CROSS + " All Parquet partitions have the same schema.");
            parquets.get(0).printSchema(System.out);
        } else {
            System.out.println(UNICODE_LARGE_YELLOW_SQUARE + " Parquet schemas differences found.");
            System.out.println("Reference schema:");
            parquets.get(0).printSchema(System.out);
            schemasDifferences.forEach(diff -> diff.printDifferences(System.out));
        }
    }

    private long countNumberOfPartitions(List<ParquetDetails> parquets) {
        return parquets.stream().map(ParquetDetails::getPartitions).distinct().count();
    }

    private long countNumberOfRows(List<ParquetDetails> parquets) {
        return parquets.stream().mapToLong(ParquetDetails::getNumRows).sum();
    }

    private static FileSystem initFileSystem() throws IOException {
        return FileSystem.get(new Configuration());
    }

    private static String toUnicodeString(int code) {
        return new String(Character.toChars(code));
    }

    /**
     * @param args CLI arguments.
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: parquetdiff <parquet-path>");
            System.exit(1);
        }

        try {
            FileSystem fileSystem = initFileSystem();
            Main main = new Main(new ParquetReader(fileSystem));
            main.run(args[0]);
        } catch (IOException e) {
            LOGGER.error("Error occurred", e);
            System.exit(1);
        }
    }
}
