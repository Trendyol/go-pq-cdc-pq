# go-pq-cdc-pq

PostgreSQL Change Data Capture (CDC) library for Go.

## Installation

```bash
go get github.com/Trendyol/go-pq-cdc-pq
```

## Usage

See examples in the `example/` directory. Snapshot bootstrapping from `go-pq-cdc` v1.0.3+ is supported via the `cdc.snapshot` block in your config. For advanced options (`mode`, `chunkSize`, etc.) check the upstream [snapshot feature guide](https://github.com/Trendyol/go-pq-cdc/blob/main/docs/SNAPSHOT_FEATURE.md).

### Replication safety

- Target apply failures stop the sink without acknowledging pending writes and are reported through `WaitForShutdown`. Restart with the same replication slot after the target recovers.
- CDC heartbeats must be disabled: the current upstream version auto-acknowledges them outside the target sink and can skip unapplied changes.

Startup/shutdown coordination, target connection timeouts, and durable snapshot progress are separate follow-ups. This change preserves the caller-managed lifecycle: run `Start` in a goroutine so `WaitForShutdown` can observe target failures, then cancel ingestion and call `Close`. Snapshot completion metadata remains managed by upstream and is not covered by the WAL replay guarantee.

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

## License

MIT
