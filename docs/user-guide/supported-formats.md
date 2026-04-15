# Supported Formats

croissant-baker detects file types automatically. Unrecognized files are skipped silently. Handlers are checked in the order listed below — the first match wins.

## File types

--8<-- "_generated/formats-table.md"

## CSV and TSV

CSV and TSV files are read with PyArrow's streaming reader — memory is constant regardless of file size. Type inference runs in two passes: an initial sweep, then per-column promotion when the first pass hits a type conflict.

Row counts are omitted by default for speed. Pass `--count-csv-rows` to do a full scan for exact counts (slow on large datasets).

Compressed variants (`.gz`, `.bz2`, `.xz`) are handled transparently.

## FHIR (`.ndjson`, `.json` Bundle)

Two FHIR serialization formats are supported:

- **NDJSON bulk export** (`.ndjson`, `.ndjson.gz`): one resource per line, all of the same `resourceType`. Produced by FHIR Bulk Data servers.
- **JSON Bundle** (`.json`, `.json.gz`): a FHIR Bundle whose `entry[]` may contain mixed resource types.

Field names and types are inferred from a sample of resources. `OperationOutcome` resources (error markers) are skipped.

!!! note
    FHIR `.json` files are detected by content — the handler looks for `"resourceType": "<UpperCase…"` before accepting. Plain JSON files that happen to use `.json` are handled by the JSON handler instead.

## JSON and JSONL

- **JSON** (`.json`, `.json.gz`): an array of objects (one object per record) or a single object (treated as one record).
- **JSONL** (`.jsonl`, `.jsonl.gz`): newline-delimited JSON, one object per line.

Schema is inferred from a sample of records. FHIR `.json` files are excluded — they go to the FHIR handler.

## Parquet

Schema is read from Parquet metadata only — the file data is never loaded. Partitioned datasets (a directory containing two or more `.parquet` files) are grouped into a single logical `cr:FileSet` and `cr:RecordSet`.

## WFDB

WFDB (WaveForm DataBase) is the standard physiological signal format on PhysioNet. The handler reads the `.hea` header file and records signal channel names, sampling frequency, number of samples, and duration. Associated `.dat` binary files are listed as related files.

## Images

Standard images are read with Pillow. Multi-band or scientific TIFFs fall back to `tifffile`. All images in a dataset are grouped into one `cr:FileSet` with a single summary `cr:RecordSet` covering width, height, color mode, and encoding format.

## Hidden files and directories

Files inside hidden directories (any path component starting with `.`) are always skipped. Use `--include` and `--exclude` glob patterns to further control which files are processed.
