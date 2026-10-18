# cmr-s3

Use Cloud Mail.ru storage with S3 tools such as AWS CLI and rclone.
Files, bucket names, object names, and metadata are encrypted before being stored in the cloud.

For a container-based setup without a host Rust installation, see [Run with Docker](#run-with-docker).

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
Keep the key file private.

### 2. Configure the server and buckets

Edit `.cmr-s3/buckets.toml` using the [commented example](examples/buckets.toml):

- Set your Cloud Mail.ru login and application password in `[server]`.
- Keep the buckets you need and give each a unique name, access key, and secret key.
  These S3 credentials must differ from your Cloud Mail.ru application password.

The server creates the configured buckets automatically. Each credential pair can access only its own bucket.

### 3. Start the server

```sh
./target/release/cmr-s3 serve --config .cmr-s3/buckets.toml
```

For a first run, use a cloud folder that is empty or does not exist. For a nested path, create its parent first.

## Run with Docker

Build locally for a new installation (run as a non-root host user):

```sh
image=cmr-s3:local
docker build --tag "$image" .
mkdir -m 700 .cmr-s3 .cmr-s3/state
install -m 600 examples/buckets.toml .cmr-s3/buckets.toml
docker run --rm --user "$(id -u):$(id -g)" \
  -v "$PWD/.cmr-s3:/config" "$image" keygen /config/master.key
```

Edit `.cmr-s3/buckets.toml` with your cloud and bucket credentials, as in the Quick start.
**Back up `master.key` separately.** For existing data, reuse its key, configuration, and state.

```sh
docker run --rm --name cmr-s3 --user "$(id -u):$(id -g)" \
  --read-only --stop-timeout 40 \
  -p 127.0.0.1:9000:9000 \
  -v "$PWD/.cmr-s3:/etc/cmr-s3:ro" \
  -v "$PWD/.cmr-s3/state:/var/lib/cmr-s3" \
  "$image"
```

Keep `.cmr-s3` private and persistent. The published port is host-local only.
After a [release](#releases), you can use `ghcr.io/velikodniy/cmr-s3:0.1.0` instead of the local image (Linux amd64/arm64).

## Connect a client

| Setting | Value |
| --- | --- |
| Endpoint | `http://127.0.0.1:9000` |
| Region | `us-east-1` |
| Credentials | The access key and secret key for your bucket |
| Addressing | Path-style |

The examples below assume the client runs on the server host.
For remote access, use the server's address and protect the connection with a TLS proxy. Do not expose the plain HTTP endpoint to the internet.

Current source supports signed empty requests behind proxies such as Caddy that omit `Content-Length: 0`. The server restores zero length only when HTTP framing proves an empty body, with no transfer encoding or declared trailer, before performing normal signature verification. A missing nonzero signed length or an invalid signature is still rejected. The published 0.1.0 container predates this fix; build current source when using affected AWS CLI downloads through a proxy.

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

For all server options:

```sh
./target/release/cmr-s3 serve --help
```

## Development

Run `cargo fetch --locked` once before these checks. Regular tests need no cloud account;
live cloud and external-client tests are opt-in.

```sh
cargo test --workspace --offline
cargo fmt --all --check
cargo clippy --workspace --all-targets --offline -- -D warnings
```

## Releases

Stable `vX.Y.Z` tags publish `ghcr.io/velikodniy/cmr-s3:<version>` and `:latest` for Linux amd64/arm64.
The tag must match the `cmr-s3` package version and point to a commit reachable from `main`.
See the [release workflow](.github/workflows/release.yml) for publishing details.
