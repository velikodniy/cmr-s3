# Storage layout

This layout belongs to cmr-s3, not to `s3s`.
The server owns one cloud folder, set by `server.cloud_root` (default: `/cmr-s3`).
Run only one server for this folder. Do not edit, move, or delete its files with other cloud clients.

## Cloud files

Paths below are relative to the cloud folder.
All file contents are encrypted, including metadata.

| Path | Contents |
| --- | --- |
| `format` | Storage version and a unique storage ID. |
| `buckets/<name>.bin` | Bucket name, ID, and creation time. |
| `objects/<name>.bin` | Object manifest: bucket, exact S3 key, size, ETag, timestamps, metadata, checksums, part information, and a content-layout ID. |
| `layouts/<id>.bin` | Content layout: chunk size and total size, or an ordered list of ranges from other layouts. |
| `data/<id>/created.bin` | Creation time for a content group. |
| `data/<id>/00000000.bin`, `00000001.bin`, … | Content chunks, numbered from zero. Each cloud chunk file is at most 64 MiB, including encryption overhead. |

`<name>` is a 64-character keyed hash, not the bucket name or S3 key.
`<id>` is a random UUID. A simple layout uses the same ID as its data folder.
Metadata records contain encrypted JSON; chunk files contain encrypted object bytes.

S3 keys do not map to cloud paths. Copies and multipart objects can share stored content through layouts.
S3 listings show committed objects, not these internal files.
An empty bucket still has a bucket record.

## Local files

The private directory set by `server.state_dir` contains:

- `index.sqlite3`: bucket and object records, cached layouts, unfinished multipart uploads, and a journal of pending changes. SQLite can also create `-wal` and `-shm` files.
- `stage/`: temporary incoming request bodies.
- `lock`: prevents two processes from using the same local state directory.

Local state can contain plaintext. Keep it private.
Keep the master key outside the cloud folder and back it up separately.

## Recovery and cleanup

If the index is lost, startup rebuilds it from cloud bucket records and object manifests.
Use the original master key, cloud folder, and bucket configuration.
Use a new state directory, or pass `--rebuild-index` to rebuild the existing catalog.
Unfinished multipart uploads survive normal restarts, but not loss of the index.

Rebuilding does not delete cloud files or reset their age.
Cleanup runs hourly. It keeps content referenced by committed objects or active multipart uploads; active operations and reads delay cleanup.
By default, inactive uploads expire after seven days, and unused content is kept for at least 24 hours.
These periods are configurable.

Without an object manifest, leftover content does not identify its original S3 key.
It can be removed by cleanup, but rebuilding cannot restore it as a named object.
