const duckdb = require("duckdb");
const path = require("path");

const CSV_FILE = path.join(__dirname, "Atviri_TP_parko_duomenys.csv");
const PARQUET_FILE = path.join(__dirname, "vehicles.parquet");

const db = new duckdb.Database(":memory:");

db.run(
  `COPY (
    SELECT * FROM read_csv('${CSV_FILE.replace(/'/g, "''")}', header=true, all_varchar=true)
  ) TO '${PARQUET_FILE.replace(/'/g, "''")}' (FORMAT PARQUET, COMPRESSION ZSTD);`,
  (err) => {
    if (err) {
      process.stderr.write("Conversion failed: " + err.message + "\n");
      process.exit(1);
    }
    db.close();
  }
);
