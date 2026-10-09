# go-pq-cdc-pq

PostgreSQL Change Data Capture (CDC) library for Go.

## Installation

```bash
go get github.com/Trendyol/go-pq-cdc-pq
```

## Usage

See examples in the `example/` directory. Snapshot bootstrapping from `go-pq-cdc` v1.0.3+ is supported via the `cdc.snapshot` block in your config. For advanced options (`mode`, `chunkSize`, etc.) check the upstream [snapshot feature guide](https://github.com/Trendyol/go-pq-cdc/blob/main/docs/SNAPSHOT_FEATURE.md).

### Table primary key mapping

By default, upsert/delete queries use primary key column name `id`. If you have multiple tables with different primary key names, you can provide a per-table mapping via config (`tablePrimaryKeys`) or via options (`WithTablePrimaryKeys`).

Config example:

```yaml
tablePrimaryKeys:
  users: id
  mock_data_table: id_pk
```

Option example:

```go
gopqcdcpq.WithTablePrimaryKeys(map[string]string{
  "users":          "id",
  "mock_data_table": "id_pk",
})
```

## Change Metadata

`Message.LSN` is the WAL position of the change in PostgreSQL `X/Y` form. It increases with every change to a given row, so a sink that stores it per row can skip a change older than the one it already has. Snapshot events carry the LSN the snapshot was taken at, shared by every snapshot row.

`Message.TransactionID` is the top-level transaction that produced the change, and `0` for snapshot events. Use it to group related changes from one transaction; compare `LSN` to order changes to the same row across transactions. `TransactionID` is 32-bit and wraps around, so use it to group changes, not to order them.

## License

MIT
