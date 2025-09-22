ParquetDiff
======

Find schema differences in a partitioned Parquet directory.

[![Codacy Badge](https://app.codacy.com/project/badge/Grade/a95e487441c04ceea58d5d245c86040c)](https://app.codacy.com/gh/romibuzi/ParquetDiff/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)

#### Features

- **Lightweight**: Uses only Parquet and Hadoop common Java libraries.
- **Schema diffing**: Detects differences in Parquet individual files schemas, including nested structures.
- **Metadata-only parsing**: Reads only Parquet footer metadata — no data is loaded.
- **Embeddable**: Use CLI tool or as a library, making it ideal for validations and CI checks.

### CLI Usage

```
java -jar parquetdiff.jar /path/to/data.parquet # local

java -jar parquetdiff.jar hdfs:///path/to/data.parquet # hdfs
```

#### Example

```
12:10:38.516 INFO  io.github.romibuzi.parquetdiff.Main - Found 527 partitions and 527 parquets files
12:10:38.520 INFO  io.github.romibuzi.parquetdiff.Main - Total rows: 10000
✅ All Parquet partitions have the same structure.
🟨 Parquet schemas differences found.
Reference schema:
root: 
  |-- location: string (binary)
  |-- vaccine: string (binary)
  |-- source_url: string (binary)
  |-- total_vaccinations: int32
  |-- people_vaccinated: int32
  |-- people_fully_vaccinated: int32
  |-- total_boosters: int32
Differences found in [date=2022-05-28], compared to [date=2020-12-28]:
missing field: 'root.people_vaccinated'.
Differences found in [date=2022-05-29], compared to [date=2022-05-28]:
additional field: 'root.people_vaccinated'.
```

### Library Usage

add dependency with maven:

```
<dependency>
    <groupId>io.github.romibuzi</groupId>
    <artifactId>parquetdiff</artifactId>
    <version>1.2.0</version>
</dependency>
```

or gradle: `implementation("io.github.romibuzi:parquetdiff:1.2.0")`

usage:

```java
import io.github.romibuzi.parquetdiff.ParquetReader;
import io.github.romibuzi.parquetdiff.diff.ParquetComparator;
import io.github.romibuzi.parquetdiff.diff.ParquetSchemaDiff;
import io.github.romibuzi.parquetdiff.metadata.ParquetDetails;

import java.io.IOException;

class ExampleUsage {
  public static void main(String[] args) throws IOException {
    ParquetReader reader = ParquetReader.getDefault();
    List<ParquetDetails> parquets = reader.readParquetDirectory("my_data.parquet");
    List<ParquetSchemaDiff> diffs = ParquetComparator.findSchemasDifferences(parquets);
    for (ParquetSchemaDiff diff : diffs) {
      // prints a summary of all differences found
      diff.print(System.out);
      
      // or print each of them
      System.out.println("differences between " + diff.getFirst() + " and " + diff.getSecond());
      System.out.println(diff.getAdditionalNodes());
      System.out.println(diff.getMissingNodes());
      System.out.println(diff.getTypeDiffs());
      System.out.println(diff.getPrimitiveTypeDiffs());
      System.out.println(diff.getRepetitionDiffs());
    }
  }
}
```

See the [Wiki](https://github.com/romibuzi/ParquetDiff/wiki) for more examples.

### Requirements

- Java **11 or higher**
- Maven

### Build from source

```
git clone https://github.com/romibuzi/ParquetDiff.git
cd ParquetDiff
mvn package
```

Then run:

`java -jar cli/target/parquetdiff.jar /path/to/data.parquet`

### License

Licensed under the MIT license. See [LICENSE](LICENSE) for the full details.
