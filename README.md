# LT Park Analyzer

Browser-based Lithuanian vehicle registry data explorer. Zero backend — runs entirely in the browser using DuckDB-WASM and a pre-converted Parquet file.

## DATA

Used 2026-01-26 https://www.regitra.lt/imone/atviri-duomenys/

## Setup

```bash
npm install
node convert.js    # converts CSV → vehicles.parquet (~78MB)
npx serve .        # start static server
```

Open http://localhost:3000

## How it works

1. **`convert.js`** — one-time Node.js script that converts `Atviri_TP_parko_duomenys.csv` (918MB, 2.4M rows, 69 columns) into `vehicles.parquet` using DuckDB with ZSTD compression. Adds computed columns:

   - **`AGR_MARKE`** — normalized brand name. The raw `MARKE` field has hundreds of variants (e.g. 62 for Volkswagen: `VW`, `VOLKSWAGEN`, `VOLKSWAGEN-VW`, `VW 1K ABBKCX0...`, etc.). `AGR_MARKE` maps them all to a single canonical name (`VOLKSWAGEN`). Explicit rules handle multi-word brands (MERCEDES-BENZ, LAND ROVER, ALFA ROMEO, ROLLS-ROYCE, ASTON MARTIN, HARLEY-DAVIDSON, etc.) and merges (VW/VOLKSWAGEN, DAIMLER/EVOBUS/DAIMLERCHRYSLER → MERCEDES-BENZ). Remaining brands fall back to extracting the first word, uppercased.

   - **`AGR_CAR_YEAR`** — vehicle year. Uses `MODELIO_METAI` when available, otherwise extracts the year from `PIRM_REG_DATA` (first registration date).

2. **`index.html`** — single-page app that loads DuckDB-WASM from CDN, fetches the Parquet file, and provides:
   - **5 main filters** (cascading): Markė (AGR_MARKE), Komercinis pav., Gamintojo pav., Gamintojo pav. baz., Modelio metai — searchable dropdowns with type-to-filter
   - **5 extra filters** (toggle "Daugiau filtrų"):
     - Darbinis tūris, Galia, Rida, Maks. greitis — numeric inputs supporting operators (`>=1500`, `<3000`, `=110`, `200`)
     - Savivaldybė — searchable dropdown
   - **Results table** with 14 key columns
   - **Row detail modal** — click any row to see all 69+ columns
   - **Pagination** (50 rows per page)
   - **Progress bar** during Parquet file download
   - **Row count and query time** display

## Data source

[Atviri transporto priemonių parko duomenys](https://data.gov.lt/) — Lithuanian open vehicle registry data.
