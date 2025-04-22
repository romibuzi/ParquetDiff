package io.github.romibuzi.parquetdiff.metadata;

import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents a list of chained partitions.
 */
public final class ParquetPartitions {
    private final List<ParquetPartition> partitions;

    private ParquetPartitions(List<ParquetPartition> partitions) {
        this.partitions = partitions;
    }

    /**
     * Parses the given {@link Path} to extract partitions from folders formatted as /partition=value/.
     *
     * @param path The full path to a Parquet file.
     * @return A {@link ParquetPartitions} instance with extracted partitions.
     */
    static ParquetPartitions fromPath(Path path) {
        List<ParquetPartition> partitions =
                Arrays.stream(path.toUri().getPath().split("/"))
                        .filter(part -> part.contains("="))
                        .map(ParquetPartition::fromString)
                        .collect(Collectors.toList());

        return new ParquetPartitions(partitions);
    }

    /**
     * @return All partitions.
     */
    public List<ParquetPartition> getPartitions() {
        return partitions;
    }

    /**
     * @return All partition keys.
     */
    public List<String> getKeys() {
        return partitions.stream().map(ParquetPartition::getKey).collect(Collectors.toList());
    }

    /**
     * @return All partition values.
     */
    public List<String> getValues() {
        return partitions.stream().map(ParquetPartition::getValue).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return partitions.stream()
                .map(ParquetPartition::toString)
                .collect(Collectors.joining("/", "[", "]"));
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParquetPartitions that = (ParquetPartitions) o;
        return Objects.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(partitions);
    }
}
