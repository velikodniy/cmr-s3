# cmr-s3

Use Cloud Mail.ru storage with S3 tools such as AWS CLI and rclone.
Files, bucket names, object names, and metadata are encrypted before being stored in the cloud.

## Quick start

You need **Rust 1.99 or later**, a Cloud Mail.ru account, and an application password.
Run these steps on the server host, from this repository.

### 1. Build and prepare

```sh
cargo build --release -p cmr-s3
mkdir -m 700 .cmr-s3
./target/release/cmr-s3 keygen .cmr-s3/master.key
install -m 600 examples/buckets.toml .cmr-s3/buckets.toml
```

**Back up `master.key` separately from Cloud Mail.ru. Without it, your stored data cannot be recovered.**
The key-generation command will not overwrite an existing key.

`master.key` is an ASCII JSON Web Key (JWK): `kty` is `oct`, and `k` contains the 256-bit secret in unpadded base64url.
The file is not encrypted; keep it owner-only.

### 2. Configure the server and buckets

Edit `.cmr-s3/buckets.toml` using the [example configuration](examples/buckets.toml):

- In `[server]`, set your Cloud Mail.ru login, application password, and `key_file` path.
- Adjust the cloud folder, local state directory, or listen address if needed.

- Keep only the buckets you need, such as `backups`.
- Replace all example credentials with private values.
- Give each bucket a unique name, access key, and secret key.
- Keep these S3 credentials separate from your Cloud Mail.ru application password.

The server creates the configured buckets automatically. Each credential pair can access only its own bucket.

### 3. Start the server

```sh
./target/release/cmr-s3 serve --config .cmr-s3/buckets.toml
```

Settings are applied in this order: **command-line options → environment variables → config file → defaults**.
For example, `CMR_S3_KEY_FILE` or `--key-file` can override `[server].key_file`.
Relative local paths are resolved from the server's working directory.

For a first run, use a cloud folder that is empty or does not exist. For a nested path, create its parent first.

## Connect a client

| Setting | Value |
| --- | --- |
| Endpoint | `http://127.0.0.1:9000` |
| Region | `us-east-1` |
| Credentials | The access key and secret key for your bucket |
| Addressing | Path-style |

The examples below assume the client runs on the server host.
For remote access, use the server's address and protect the connection with a TLS proxy. Do not expose the plain HTTP endpoint to the internet.

### AWS CLI

```sh
export AWS_ACCESS_KEY_ID='your-backups-access-key'
export AWS_SECRET_ACCESS_KEY='your-backups-secret-key'
export AWS_DEFAULT_REGION='us-east-1'

aws --endpoint-url http://127.0.0.1:9000 s3 cp ./archive.tar s3://backups/archive.tar
```

### rclone

Add this remote to your rclone configuration, replacing the credentials:

```ini
[cmr]
type = s3
provider = Other
access_key_id = your-backups-access-key
secret_access_key = your-backups-secret-key
endpoint = http://127.0.0.1:9000
region = us-east-1
force_path_style = true
no_check_bucket = true
```

Then upload your files:

```sh
rclone copy ./backup cmr:backups
```

`no_check_bucket` prevents rclone from trying to create a bucket; buckets are managed in the server configuration.

## Manage and protect your data

- Run **one active server per cloud folder**, even across different hosts.
- Do not change files inside that folder through other Cloud Mail.ru clients.
- Keep `.cmr-s3` private: local working files can contain unencrypted data.
- Back up your bucket configuration as well as your encryption key.

| Task | What to do |
| --- | --- |
| Add a bucket or rotate its S3 credentials | Edit `.cmr-s3/buckets.toml` and restart the server. |
| Revoke access to a bucket | Remove its configuration entry and restart. Its stored data is not deleted. |
| Restart normally | Reuse the same key, cloud folder, and local state directory. |
| Recover after losing local state | Set `[server].state_dir` to a new directory and restart with the original key and cloud folder. Completed uploads are recovered; unfinished multipart uploads are lost. |
| Rebuild existing local state | Start with `./target/release/cmr-s3 serve --config .cmr-s3/buckets.toml --rebuild-index`. |
| Server refuses requests after a failed write | Restart it so it can check the stored state before accepting requests again. |

## Support and limits

| Area | What to expect |
| --- | --- |
| S3 features | Uploads, downloads, copies within a bucket, deletion, listings, metadata, ranges, checksums, and multipart uploads. |
| Bucket management | Configuration file only; clients cannot create or delete buckets. |
| Encryption | Always enabled for cloud data. Cloud Mail.ru can still see file sizes and access patterns. |
| Not supported | Public access, cross-bucket copies, versioning, lifecycle rules, bucket policies, and AWS KMS. |
| Upload size | Up to 5 GiB per request or multipart part. |
| Local upload space | 20 GiB by default. |
| Abandoned multipart uploads | Expire after seven days by default. |

For all server options:

```sh
./target/release/cmr-s3 serve --help
```

## Development

The workspace contains `cmr-api` (the Cloud Mail.ru client) and `cmr-s3` (the S3 server).
Run `cargo fetch --locked` once to download all dependencies, including test dependencies, before using the offline checks below.
The regular tests run locally without a cloud account; live and external-client tests are skipped by default.

```sh
cargo test --workspace --offline
cargo fmt --all --check
cargo clippy --workspace --all-targets --offline -- -D warnings
```

With rclone and Docker Desktop installed, run the client compatibility checks:

```sh
cargo test --offline -p cmr-s3 --test clients -- --ignored --nocapture
```

To test against Cloud Mail.ru, set `CMR_LOGIN` and `CMR_TOKEN` to your login and application password, then run:

```sh
cargo test --offline -p cmr-s3 --test live live_signed_s3_round_trip_and_recovery -- --ignored --nocapture
```

The live test writes to a temporary cloud folder. It uses `CMR_TOKEN`, while the server uses `CMR_PASSWORD`.
