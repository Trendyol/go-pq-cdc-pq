# go-pq-cdc-pq

PostgreSQL Change Data Capture (CDC) library for Go.

## Installation

```bash
go get github.com/Trendyol/go-pq-cdc-pq
```

## Usage

See examples in the `example/` directory. Snapshot bootstrapping from `go-pq-cdc` v1.0.3+ is supported via the `cdc.snapshot` block in your config. For advanced options (`mode`, `chunkSize`, etc.) check the upstream [snapshot feature guide](https://github.com/Trendyol/go-pq-cdc/blob/main/docs/SNAPSHOT_FEATURE.md).

## License

MIT
