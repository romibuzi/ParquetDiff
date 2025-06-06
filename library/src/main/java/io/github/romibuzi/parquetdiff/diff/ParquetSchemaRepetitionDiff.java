package io.github.romibuzi.parquetdiff.diff;

import io.github.romibuzi.parquetdiff.metadata.ParquetSchemaNodePath;
import org.apache.parquet.schema.Type;

import java.util.Objects;

/**
 * Represents a Repetition difference, ex: REQUIRED vs OPTIONAL.
 */
public final class ParquetSchemaRepetitionDiff {
    private final ParquetSchemaNodePath nodePath;
    private final Type.Repetition oldRepetition;
    private final Type.Repetition newRepetition;

    /**
     * @param nodePath      The path to the node in the schema.
     * @param oldRepetition The repetition associated to that node in the previous schema.
     * @param newRepetition The repetition associated to that node in the new schema.
     */
    public ParquetSchemaRepetitionDiff(ParquetSchemaNodePath nodePath,
                                       Type.Repetition oldRepetition,
                                       Type.Repetition newRepetition) {
        this.nodePath = nodePath;
        this.oldRepetition = oldRepetition;
        this.newRepetition = newRepetition;
    }

    public ParquetSchemaNodePath getNodePath() {
        return nodePath;
    }

    public Type.Repetition getOldRepetition() {
        return oldRepetition;
    }

    public Type.Repetition getNewRepetition() {
        return newRepetition;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParquetSchemaRepetitionDiff that = (ParquetSchemaRepetitionDiff) o;
        return Objects.equals(nodePath, that.nodePath) && oldRepetition == that.oldRepetition
                && newRepetition == that.newRepetition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodePath, oldRepetition, newRepetition);
    }
}
