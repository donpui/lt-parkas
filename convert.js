const duckdb = require("duckdb");
const path = require("path");

const CSV_FILE = path.join(__dirname, "Atviri_TP_parko_duomenys.csv");
const PARQUET_FILE = path.join(__dirname, "vehicles.parquet");

const db = new duckdb.Database(":memory:");

// Normalize MARKE into AGR_MARKE:
// 1. First, check explicit multi-word brand mappings
// 2. Then extract the first word (split on space, parenthesis, slash, dot, comma)
// 3. Uppercase and trim
const AGR_MARKE_SQL = `
  CASE
    WHEN UPPER(MARKE) LIKE '%MERCEDES%'       THEN 'MERCEDES-BENZ'
    WHEN UPPER(MARKE) LIKE 'DAIMLER%'          THEN 'MERCEDES-BENZ'
    WHEN UPPER(MARKE) LIKE 'EVOBUS%'           THEN 'MERCEDES-BENZ'
    WHEN UPPER(MARKE) LIKE 'VW%'               THEN 'VOLKSWAGEN'
    WHEN UPPER(MARKE) LIKE 'VOLKSWAGEN%'       THEN 'VOLKSWAGEN'
    WHEN UPPER(MARKE) LIKE 'DAIMLERCHRYSLER%'  THEN 'MERCEDES-BENZ'
    WHEN UPPER(MARKE) LIKE 'LAND ROVER%'       THEN 'LAND ROVER'
    WHEN UPPER(MARKE) LIKE 'ALFA ROMEO%'       THEN 'ALFA ROMEO'
    WHEN UPPER(MARKE) LIKE 'ROLLS ROYCE%'      THEN 'ROLLS-ROYCE'
    WHEN UPPER(MARKE) LIKE 'ROLLS-ROYCE%'      THEN 'ROLLS-ROYCE'
    WHEN UPPER(MARKE) LIKE 'ASTON MARTIN%'     THEN 'ASTON MARTIN'
    WHEN UPPER(MARKE) LIKE 'DE TOMASO%'        THEN 'DE TOMASO'
    WHEN UPPER(MARKE) LIKE 'CAN-AM%'           THEN 'CAN-AM'
    WHEN UPPER(MARKE) LIKE 'HARLEY%'           THEN 'HARLEY-DAVIDSON'
    WHEN UPPER(MARKE) LIKE 'DR MOTOR%'         THEN 'DR MOTOR'
    WHEN UPPER(MARKE) LIKE 'EL DETHLEFFS%'     THEN 'DETHLEFFS'
    WHEN UPPER(MARKE) LIKE 'GENERAL MOTORS%'   THEN 'GENERAL MOTORS'
    ELSE UPPER(TRIM(REGEXP_EXTRACT(MARKE, '^([A-Za-z0-9À-ž][-A-Za-z0-9À-ž]*)', 1)))
  END
`;

// Combine MODELIO_METAI and PIRM_REG_DATA into AGR_CAR_YEAR:
// Use MODELIO_METAI if available, otherwise extract year from PIRM_REG_DATA
const AGR_CAR_YEAR_SQL = `
  CASE
    WHEN MODELIO_METAI IS NOT NULL AND TRIM(MODELIO_METAI) != ''
      THEN TRIM(MODELIO_METAI)
    WHEN PIRM_REG_DATA IS NOT NULL AND TRIM(PIRM_REG_DATA) != ''
      THEN SUBSTRING(TRIM(PIRM_REG_DATA), 1, 4)
    ELSE NULL
  END
`;

db.run(
  `COPY (
    SELECT *,
      ${AGR_MARKE_SQL} AS AGR_MARKE,
      ${AGR_CAR_YEAR_SQL} AS AGR_CAR_YEAR
    FROM read_csv('${CSV_FILE.replace(/'/g, "''")}', header=true, all_varchar=true)
  ) TO '${PARQUET_FILE.replace(/'/g, "''")}' (FORMAT PARQUET, COMPRESSION ZSTD);`,
  (err) => {
    if (err) {
      process.stderr.write("Conversion failed: " + err.message + "\n");
      process.exit(1);
    }
    db.close();
  }
);
