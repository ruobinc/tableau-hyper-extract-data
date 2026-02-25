# tableau-hyper-extract-data

Download a datasource from Tableau Cloud/Server and extract data from its embedded Hyper file.

## What it does

1. Downloads a `.tdsx` file from Tableau Server via the Tableau Server Client (TSC)
2. Extracts the embedded `.hyper` file from the archive
3. Reads and prints the schema and all rows using the Hyper API

## Setup

```bash
cp .env.example .env
# Fill in your credentials in .env
uv sync
```

## Environment variables

| Variable | Description |
|---|---|
| `SERVER_NAME` | Tableau Server URL |
| `SITE_NAME` | Site name (content URL) |
| `PAT_NAME` | Personal Access Token name |
| `PAT_VALUE` | Personal Access Token value |
| `DATASOURCE_ID` | ID of the published datasource |

## Run

```bash
uv run extract_hyperfile.py
```

## References

- [Tableau Hyper API samples](https://github.com/tableau/hyper-api-samples)
- [Hyper API – Reading data](https://tableau.github.io/hyper-db/docs/guides/hyper_file/read)
