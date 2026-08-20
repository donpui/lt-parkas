const duckdb = require("duckdb");
const path = require("path");

const CSV_FILE = path.join(__dirname, "Atviri_TP_parko_duomenys.csv");
const PARQUET_FILE = path.join(__dirname, "vehicles.parquet");

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
    WHEN GAMYBOS_METAI IS NOT NULL AND TRIM(GAMYBOS_METAI) != ''
      THEN TRIM(GAMYBOS_METAI)
    WHEN MODELIO_METAI IS NOT NULL AND TRIM(MODELIO_METAI) != ''
      THEN TRIM(MODELIO_METAI)
    WHEN PIRM_REG_DATA IS NOT NULL AND TRIM(PIRM_REG_DATA) != ''
      THEN SUBSTRING(TRIM(PIRM_REG_DATA), 1, 4)
    WHEN PIRM_REG_DATA_LT IS NOT NULL AND TRIM(PIRM_REG_DATA_LT) != ''
      THEN SUBSTRING(TRIM(PIRM_REG_DATA_LT), 1, 4)
    ELSE NULL
  END
`;

// A commercial name sometimes repeats the make, for example:
//   MARKE=TOYOTA, KOMERCINIS_PAV="TOYOTA PRIUS PLUS"
// Normalize case and whitespace, then remove the make only when it is a
// complete prefix followed by a separator. The original value is retained as
// RAW_KOMERCINIS_PAV for traceability.
function hasCommercialPrefix(prefixSql) {
  return `
    LEFT(CLEAN_KOMERCINIS_PAV, LENGTH(${prefixSql})) = ${prefixSql}
    AND LENGTH(CLEAN_KOMERCINIS_PAV) > LENGTH(${prefixSql})
    AND SUBSTRING(CLEAN_KOMERCINIS_PAV, LENGTH(${prefixSql}) + 1, 1) IN (' ', '-', '/', ':', ',', '.', ';')
  `;
}

const REDUNDANT_MARKE_PREFIX_SQL = `
  CASE
    WHEN CLEAN_KOMERCINIS_PAV IS NULL OR AGR_MARKE IS NULL THEN NULL
    WHEN ${hasCommercialPrefix('AGR_MARKE')} THEN AGR_MARKE
    WHEN AGR_MARKE = 'VOLKSWAGEN' AND ${hasCommercialPrefix("'VW'")} THEN 'VW'
    WHEN AGR_MARKE = 'MERCEDES-BENZ' AND ${hasCommercialPrefix("'MERCEDES BENZ'")} THEN 'MERCEDES BENZ'
    WHEN AGR_MARKE = 'MERCEDES-BENZ' AND ${hasCommercialPrefix("'MERCEDES'")} THEN 'MERCEDES'
    WHEN AGR_MARKE = 'MERCEDES-BENZ' AND ${hasCommercialPrefix("'DAIMLER'")} THEN 'DAIMLER'
    WHEN AGR_MARKE = 'ROLLS-ROYCE' AND ${hasCommercialPrefix("'ROLLS ROYCE'")} THEN 'ROLLS ROYCE'
    ELSE NULL
  END
`;

function buildNormalizedSelect(sourceExpression) {
  return `
    WITH normalized AS (
      SELECT *,
        ${AGR_MARKE_SQL} AS AGR_MARKE,
        ${AGR_CAR_YEAR_SQL} AS AGR_CAR_YEAR
      FROM ${sourceExpression}
    ), commercial_prepared AS (
      SELECT *,
        NULLIF(UPPER(TRIM(REGEXP_REPLACE(KOMERCINIS_PAV, '\\s+', ' ', 'g'))), '') AS CLEAN_KOMERCINIS_PAV
      FROM normalized
    ), commercial_prefixed AS (
      SELECT *,
        ${REDUNDANT_MARKE_PREFIX_SQL} AS REDUNDANT_MARKE_PREFIX
      FROM commercial_prepared
    )
    SELECT
      * EXCLUDE (KOMERCINIS_PAV, CLEAN_KOMERCINIS_PAV, REDUNDANT_MARKE_PREFIX),
      KOMERCINIS_PAV AS RAW_KOMERCINIS_PAV,
      CASE
        WHEN REDUNDANT_MARKE_PREFIX IS NULL THEN CLEAN_KOMERCINIS_PAV
        ELSE COALESCE(
          NULLIF(
            LTRIM(
              SUBSTRING(CLEAN_KOMERCINIS_PAV, LENGTH(REDUNDANT_MARKE_PREFIX) + 1),
              ' -/:,.;'
            ),
            ''
          ),
          CLEAN_KOMERCINIS_PAV
        )
      END AS KOMERCINIS_PAV
    FROM commercial_prefixed
  `;
}

function escapeSqlPath(filePath) {
  return filePath.replace(/'/g, "''");
}

function convert() {
  const db = new duckdb.Database(":memory:");
  const sourceExpression = `read_csv('${escapeSqlPath(CSV_FILE)}', header=true, all_varchar=true)`;
  const selectSql = buildNormalizedSelect(sourceExpression);

  db.run(
    `COPY (${selectSql}) TO '${escapeSqlPath(PARQUET_FILE)}' (FORMAT PARQUET, COMPRESSION ZSTD);`,
    (err) => {
      if (err) {
        process.stderr.write("Conversion failed: " + err.message + "\n");
        process.exitCode = 1;
      }
      db.close();
    }
  );
}

if (require.main === module) {
  convert();
}

module.exports = {
  AGR_MARKE_SQL,
  AGR_CAR_YEAR_SQL,
  buildNormalizedSelect,
};
