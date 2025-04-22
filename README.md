ParquetDiff
======

Find schema differences in a partitioned Parquet directory.

#### ✨ Features

- 📦 **Lightweight**: Uses only Parquet and Hadoop common Java libraries.
- 🧠 **Schema diffing**: Detects differences in Parquet individual files schemas, including nested structures.
- ⚡  **Metadata-only parsing**: Reads only Parquet footer metadata — no data is loaded.
- 🛠️ **Embeddable**: Use CLI tool or as a library, making it ideal for validations and CI
  checks.

### 🚀 CLI Usage

```
java -jar cli/target/parquetdiff.jar /path/to/data.parquet # local

java -jar cli/target/parquetdiff.jar hdfs:///path/to/data.parquet # hdfs
```

#### 🧾 Example

```
12:10:38.516 INFO  com.romibuzi.parquetdiff.Main - Found 527 partitions and 527 parquets files
12:10:38.520 INFO  com.romibuzi.parquetdiff.Main - Total rows: 10000
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

### 📚 Library Usage

add dependency with maven:

```
<dependency>
    <groupId>io.github.romibuzi</groupId>
    <artifactId>parquetdiff</artifactId>
    <version>1.0.0</version>
</dependency>
```

or gradle: `implementation("io.github.romibuzi:parquetdiff:1.0.0")`

usage:

```java
import io.github.romibuzi.parquetdiff.ParquetReader;
import io.github.romibuzi.parquetdiff.diff.ParquetComparator;
import io.github.romibuzi.parquetdiff.diff.ParquetSchemaDiff;
import io.github.romibuzi.parquetdiff.metadata.ParquetDetails;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

class Usage {
  public static void main(String[] args) throws IOException {
    ParquetReader reader = new ParquetReader(FileSystem.get(new Configuration()));
    List<ParquetDetails> parquets = reader.readParquetDirectory("my_data.parquet");
    List<ParquetSchemaDiff> differences = ParquetComparator.findSchemasDifferences(parquets);
    for (ParquetSchemaDiff difference : differences) {
      difference.printDifferences(System.out);
    }
  }
}
```

### ☕ Requirements

- Java **11 or higher**
- Maven

### 🔧 Build from source

```
git clone https://github.com/romibuzi/ParquetDiff.git
cd ParquetDiff
mvn package
```

Then run:

`java -jar cli/target/parquetdiff.jar /path/to/data.parquet`

### License

Licensed under the MIT license. See [LICENSE](LICENSE) for the full details.
