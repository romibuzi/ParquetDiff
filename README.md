ParquetDiff
======

Find schema differences in a partitioned Parquet directory.

#### ✨ Features

- 📦 **Framework-free**: No need for frameworks like Apache Spark — uses only the plain Parquet Java library.
- 🧠 **Schema diffing**: Detects differences in Parquet individual files schemas, including nested structures.
- ⚡  **Metadata-only parsing**: Reads only Parquet footer metadata — no data is loaded.
- 🛠️ **Lightweight & fast**: Focused on schema comparison, making it ideal for quick validations or CI checks.

#### 🚀 Usage

```
java -jar ParquetDiff.jar /path/to/data.parquet # local

java -jar ParquetDiff.jar hdfs:///path/to/data.parquet # hdfs
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

#### ☕ Requirements

- Java **17 or higher**
- Maven

#### 🔧 Build from source

```
git clone https://github.com/romibuzi/ParquetDiff.git
cd ParquetDiff
mvn package
```

Then run:

`java -jar target/ParquetDiff.jar /path/to/data.parquet`

## License

Licensed under the MIT license. See [LICENSE](LICENSE) for the full details.
