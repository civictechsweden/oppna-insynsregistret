# Öppna insynsregistret (Open Insider Trading Data)

[![Fetch data](https://github.com/civictechsweden/oppna-insynsregistret/actions/workflows/fetch.yml/badge.svg)](https://github.com/civictechsweden/oppna-insynsregistret/actions/workflows/fetch.yml)
[![Python: >= 3.10](https://img.shields.io/badge/Python->=%203.10-blue.svg)](https://python.org)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](LICENSE)
[![Data License: CC0](https://img.shields.io/badge/Data%20License-CC0-green.svg)](https://creativecommons.org/publicdomain/zero/1.0/)

Since 2016, the Swedish Financial Supervisory Authority (*Finansinspektionen*, FI) publishes an [insider trading registry](https://www.fi.se/sv/vara-register/insynsregistret/) (**Insynsregistret**), covering all transactions by Persons Discharging Managerial Responsibilities (PDMR) and their closely associated persons under the EU Market Abuse Regulation (MAR).

Unfortunately, they are not giving it the love it deserves. The user interface is arid and the possibilities to download the dataset to explore it outside are very limited.

Öppna insynsregistret is a small project that fetches the latest rows of the registry every night and publishes the complete file as a clean CSV file.

It relies on a simple Python scraper running on a Github Action.

This project is in no way affiliated to Finansinspektionen. No liability can be expected from it or them regarding the freshness and correctness of the provided information.

---

## 📊 Download the Data

| Format | File | Size | Records |
| :--- | :--- | :--- | :--- |
| **CSV** | [`data/insynsregistret.csv`](data/insynsregistret.csv) | ~41.0 MB | ~166,000+ |

> **Automated Updates:** A lightweight GitHub Actions workflow runs every night at 01:00 UTC to incrementally fetch new transactions, revisions, and cancellations, committing updates back to this repository.

---

## 📋 Data Schema

| Column | Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `Publiceringsdatum` | `TIMESTAMP` | Publication timestamp in the registry | `2026-08-18 19:33:38` |
| `Emittent` | `VARCHAR` | Company / Issuer name | `SAAB Aktiebolag` |
| `LEI-kod` | `VARCHAR` | Legal Entity Identifier (LEI) of the issuer | `549300ZHO4JCQQI13M69` |
| `Anmälningsskyldig` | `VARCHAR` | Reporting entity or individual | `Lars Svensson` |
| `Person i ledande ställning` | `VARCHAR` | Person discharging managerial responsibilities (PDMR) | `Lars Svensson` |
| `Befattning` | `VARCHAR` | Position or role | `Styrelseledamot` |
| `Närstående` | `VARCHAR` | Closely associated person indicator | `Ja` / `""` |
| `Korrigering` | `VARCHAR` | Indicates whether this is a correction of a previous report | `Ja` / `""` |
| `Beskrivning av korrigering` | `VARCHAR` | Free text explanation of correction | `Ändrat pris per aktie` |
| `Är förstagångsrapportering` | `VARCHAR` | Initial notification indicator | `Ja` / `""` |
| `Är kopplad till aktieprogram` | `VARCHAR` | Linked to share option / incentive scheme | `Ja` / `""` |
| `Karaktär` | `VARCHAR` | Nature of transaction (`Förvärv`, `Avyttring`, `Tilldelning`, `Lösen`, `Gåva`) | `Förvärv` |
| `Instrumenttyp` | `VARCHAR` | Type of financial instrument (`Aktie`, `Option`, `Teckningsoption`, etc.) | `Aktie` |
| `Instrumentnamn` | `VARCHAR` | Financial instrument name / share class | `SAAB AB ser. B` |
| `ISIN` | `VARCHAR` | International Securities Identification Number | `SE0000112385` |
| `Transaktionsdatum` | `TIMESTAMP` | Date of execution | `2026-08-17 00:00:00` |
| `Volym` | `DOUBLE` | Number of securities / volume | `328,0` |
| `Volymsenhet` | `VARCHAR` | Unit of measurement | `Antal` |
| `Pris` | `DOUBLE` | Price per unit | `686,40` |
| `Valuta` | `VARCHAR` | Transaction currency | `SEK` |
| `Handelsplats` | `VARCHAR` | Trading venue (MIC) or off-market | `NASDAQ STOCKHOLM AB` |
| `Status` | `VARCHAR` | Record status (`Aktuell`, `Reviderad`, `Makulerad`) | `Aktuell` |

---

## 🚀 Reusing the data

### Using DuckDB (SQL directly on CSV)

```sql
SELECT
    Emittent,
    "Person i ledande ställning" AS Insider,
    Karaktär,
    Volym,
    Pris,
    Valuta,
    Publiceringsdatum
FROM read_csv('data/insynsregistret.csv', delim=';', header=true, decimal_separator=',', strict_mode=true)
WHERE Karaktär = 'Förvärv'
ORDER BY Publiceringsdatum DESC
LIMIT 10;
```

### Using Python & Polars

```python
import polars as pl

df = pl.read_csv("data/insynsregistret.csv", separator=";", decimal_comma=True)

# Top 10 issuers by insider transaction count
top_issuers = (
    df.group_by("Emittent")
    .agg(pl.len().alias("transactions"))
    .sort("transactions", descending=True)
    .head(10)
)
print(top_issuers)
```

---

## 🛠️ Running Locally

This project uses [`uv`](https://docs.astral.sh/uv/) and runs on Python >= 3.10.

### Installation

```bash
git clone https://github.com/civictechsweden/oppna-insynsregistret.git
cd oppna-insynsregistret
```

### Fetching / Updating Data

```bash
# Fast incremental update (checks last 3 days and merges into CSV)
uv run run.py

# Custom lookback window for incremental update (e.g. last 30 days)
uv run run.py --lookback 30

# Full historical extraction from July 2016 to today
uv run run.py --full

# Optional: export to Parquet (requires duckdb extra)
uv run --extra parquet run.py --parquet
```

---

## 📄 License

- **Code:** [GNU Affero General Public License v3.0 (AGPL-3.0)](LICENSE)
- **Data:** [Creative Commons Zero (CC0 1.0) Public Domain Dedication](https://creativecommons.org/publicdomain/zero/1.0/) (attribution to Civic Tech Sweden is appreciated and helps potential users of your product to understand the limitations of the dataset).
