# Cloud Mail.ru drive API

This reference describes authentication, file metadata, folder lists, uploads, downloads, folder creation, and removal.

## Endpoints

Use HTTPS for all requests.

| Service | URL |
| --- | --- |
| Authentication | `https://o2.mail.ru/token` |
| JSON API (M1) | `https://cloud.mail.ru/api/m1` |
| Dispatcher | `https://dispatcher.cloud.mail.ru` |
| Binary metadata, uploads, and downloads | Service URLs from the dispatcher |

M1 paths in this document are relative to the JSON API URL.

## Authentication

Send `POST https://o2.mail.ru/token` with these headers:

```http
Content-Type: application/x-www-form-urlencoded
User-Agent: <client name and version>
```

Use one of these sets of form fields:

| Field | Sign in | Refresh a token |
| --- | --- | --- |
| `client_id` | `cloud-win` | `cloud-win` |
| `grant_type` | `password` | `refresh_token` |
| `username` | Account login | Omit |
| `password` | Application password | Omit |
| `refresh_token` | Omit | Current refresh token |

Use an application password, not the normal account password.
The token endpoint requires a `User-Agent` header.

A successful response is a JSON object with these fields:

| Field | Type | Description |
| --- | --- | --- |
| `access_token` | string | Token for API requests |
| `refresh_token` | string | Token for the next refresh request; can be absent in a refresh response |
| `token_type` | string | `Bearer` |
| `expires_in` | integer | Access token lifetime in seconds |

Refresh the access token before it expires.
If the response has no replacement refresh token, keep the current refresh token.

Send the access token in the query parameters below:

| Request | Query parameters |
| --- | --- |
| M1 | `access_token=<access token>` |
| Binary metadata, upload, or download | `token=<access token>&client_id=cloud-win` |
| Dispatcher | None |

These query parameters contain credentials.
Do not include complete request URLs in logs.

## Paths and encoding

An account path starts with `/`, for example `/photos/a.jpg`.
The root folder is `/`.
The `home` field contains an account path.

Use UTF-8 for text.
Use URL form encoding for query parameters and form bodies.
For example, encode `/photos/a b.jpg` as `%2Fphotos%2Fa+b.jpg` in a `home` field.

For download paths, use percent encoding as described in [Download a file](#download-a-file).
For binary requests, use the string format described in [Binary encoding](#binary-encoding).

Do not assume that names with different letter case identify different items.
Unicode case comparison and normalization rules are not specified.

## Select a service

Send a `GET` request to the dispatcher:

| Path | Service |
| --- | --- |
| `/m` | Binary metadata |
| `/u` | Upload |
| `/d` | Download |

The response is text.
The first field is the service URL.
Whitespace separates the fields.
Ignore the remaining fields.

Service URLs can change.
Use the dispatcher to obtain a new URL when a selected service is no longer available.

## JSON responses

M1 responses contain a JSON object with `status` and `body` fields.
The numeric `status` field gives the operation result.
The `body` field contains the result data or error details.

Check both the HTTP status and the JSON status.
HTTP success alone does not mean that the operation succeeded.
JSON status `200` means success.
JSON status `404` means that the requested path does not exist.
Do not treat status `400` as proof that a path does not exist.

An error body can be a string or an object, for example:

```json
{"status": 404, "body": {"home": {"value": "/missing", "error": "..."}}}
```

Ignore response fields that the client does not use.

### Item fields

File metadata and folder lists use these item fields:

| Field | Type | Description |
| --- | --- | --- |
| `home` | string | Absolute account path |
| `name` | string | Item name |
| `kind` | string | Provider item class |
| `type` | string | Item type, including `file` and `folder` |
| `size` | integer | File size in bytes; can be absent for a folder |
| `hash` | string, optional | Mail.ru content hash as 40 hexadecimal digits |
| `mtime` | integer, optional | Modification time in seconds since the Unix epoch |
| `count` | object, optional | Folder and file counts |

## Get item metadata

Send `GET /file` with these query parameters:

| Parameter | Value |
| --- | --- |
| `access_token` | Access token |
| `home` | Absolute path of the file or folder |
| `offset` | `0` |
| `limit` | `2147483647` |

On JSON status `200`, `body` contains one item object.

## List a folder

Send `POST /folder` with these query parameters:

| Parameter | Value |
| --- | --- |
| `access_token` | Access token |
| `offset` | Number of items to skip; start with `0` |
| `limit` | Maximum number of items to return |

Set `Content-Type: application/x-www-form-urlencoded`.
Send the folder path in the form field `home`.

On JSON status `200`, `body.list` contains an array of item objects.
The `body` object also contains folder metadata, counts, and sort and revision fields.

To request the next page, increase `offset` by the number of returned items.
An empty list has no more items at that offset.
The maximum supported page size is not specified.
Do not assume that an arbitrarily large `limit` returns all items.

Changes to the folder between requests can affect page contents and item order.
Pages are not guaranteed to describe the folder at one point in time.

## Binary encoding

Binary requests use the following types:

| Type | Encoding |
| --- | --- |
| Unsigned integer | Unsigned LEB128, with the least significant 7-bit group first |
| Timestamp | Signed 64-bit Unix time in seconds, encoded as an unsigned 64-bit bit pattern with LEB128; no ZigZag encoding |
| String | Unsigned LEB128 value of the UTF-8 byte length plus one, then the UTF-8 bytes, then a zero byte |
| Hash | 20 raw bytes, with no length prefix or terminator |

For LEB128, set bit 7 in each byte except the last byte.
For example, integer `300` becomes bytes `AC 02` in hexadecimal.
The string `/a` becomes bytes `03 2F 61 00`.

## Upload a file

An upload has two steps:

1. Send the file bytes to an upload service.
2. Bind the returned hash to an account path.

The first step alone does not create a named file in the account.

### Send the bytes

Obtain an upload URL from dispatcher path `/u`.
Send `PUT` to that URL with these query parameters:

```text
token=<access token>&client_id=cloud-win
```

Set these headers:

```http
Accept: */*
Content-Length: <file size in bytes>
```

Send the raw file bytes as the request body.

HTTP `200` or `201` indicates a successful upload.
The response is text.
Its first whitespace-separated field is the file hash as 40 hexadecimal digits.
Compare this hash with the hash calculated from the file bytes.

### Bind the file to a path

The parent folder must exist.
Obtain a metadata URL from dispatcher path `/m`.
Send `POST` to that URL with `token=<access token>&client_id=cloud-win` in the query.
Set `Content-Type: application/x-www-form-urlencoded`, even though the body is binary.

Write these fields to the binary body in this order:

| Field | Type | Value |
| --- | --- | --- |
| Operation | Unsigned integer | `103` |
| Revision | Unsigned integer | `0` |
| Path | String | Absolute file path |
| Size | Unsigned integer | File size in bytes |
| Modification time | Timestamp | File modification time |
| Reserved | Unsigned integer | `0` |
| Hash | Hash | File content hash |
| Conflict mode | Unsigned integer | `1` to overwrite an existing file |

Conflict mode `55` requests unchanged-file handling and a new name on conflict.
For this mode, append the hash and then the size, using the same types as above.
Neither mode provides a conditional write based on the previous file contents.

The first response byte is the result code:

| Code | Meaning |
| --- | --- |
| `0` | Success |
| `1` | Error; specific cause unknown |
| `4` | Unknown result |
| `5` | Wrong path |
| `7` | No free space |
| `9` | Unknown result |
| `10` | Invalid name |
| `12` | File not modified |
| `253`, `254` | Failure; specific cause unknown |

Do not treat codes `4` or `9` as proof of success.
To check the resulting file, get its metadata and compare its size and hash with the uploaded file.

## Create a folder

The parent folder must exist.
Obtain a metadata URL from dispatcher path `/m`.
Send `POST` to that URL with `token=<access token>&client_id=cloud-win` in the query.
Set `Content-Type: application/x-www-form-urlencoded`.

Write these fields to the binary body in this order:

| Field | Type | Value |
| --- | --- | --- |
| Operation | Unsigned integer | `106` |
| Revision | Unsigned integer | `0` |
| Path | String | Absolute folder path |
| Reserved | Unsigned integer | `0` |

The first response byte is the result code:

| Code | Meaning |
| --- | --- |
| `0` | Success |
| `1` | Source or parent does not exist |
| `4` | Item already exists |
| `9` | Item exists with different letter case |
| `10` | Invalid name |
| `254` | Failure; specific cause unknown |

For an existing item, get its metadata to check that it is a folder.
Interpret result codes only with the table for the requested operation.

## Calculate the Mail.ru hash

The hash is 20 bytes.
Calculate it from the complete file contents:

- For files of 0 to 20 bytes, append zero bytes until the result is 20 bytes long.
- For files larger than 20 bytes, use the formula below.

```text
SHA1(ASCII("mrCloud") || file_bytes || ASCII(decimal_file_size))
```

Here, `||` means byte concatenation.
Write the file size in bytes as decimal digits, without leading zeros, padding, or a terminator.
For text fields, encode the hash as 40 hexadecimal digits.
For binary fields, use the 20 raw bytes.

This hash identifies file content.
It does not authenticate the sender.

## Download a file

Obtain a download URL from dispatcher path `/d`.
Construct the request URL as follows:

1. Remove the initial `/` from the account path.
2. Percent-encode the UTF-8 bytes of the remaining path, including embedded slashes.
3. Append the encoded path to the service URL, with one `/` between them.
4. Add query parameters `token=<access token>&client_id=cloud-win`.

For example, `/photos/a b.jpg` becomes `photos%2Fa%20b.jpg` before you append it to the service URL.
Do not use `+` for spaces in the download path.

Send `GET` with `Accept: */*`.
HTTP `200` returns the complete file.
Compare its size and Mail.ru hash with the file metadata to check the download.

### Download a byte range

Add this header to the download request:

```http
Range: bytes=<start>-<end>
```

Offsets start at zero.
The end offset is inclusive.
For example, `Range: bytes=0-99` requests the first 100 bytes.

For HTTP `206`, check the response header:

```http
Content-Range: bytes <start>-<end>/<total file size>
```

Check that the returned offsets match the requested range.
The body length must equal `end - start + 1`.

The server can return HTTP `200` with the complete file, even after a range request.
In this case, the first response byte is at file offset zero.
Extract the requested range from the complete response.

A partial response cannot be checked with the full-file hash alone.

## Remove a file or folder

Send `POST /file/remove` with query parameter `access_token=<access token>`.
Set `Content-Type: application/x-www-form-urlencoded`.
Send the absolute path in the form field `home`.

JSON status `200` means success.
To check removal, request metadata for the same path.

This operation accepts both file and folder paths.
It has no parameter that restricts removal to files.
Removal does not guarantee immediate recovery of storage space or permanent erasure.

## Handle errors

Check HTTP status, JSON status, and binary result codes separately.
A binary result code has meaning only for its operation.
Treat an empty binary response or an unknown result code as an unconfirmed result.

| HTTP status | Meaning |
| --- | --- |
| `401`, `403` | Authentication or access problem |
| `429` | Too many requests |
| `5xx` | Server error |

If a connection fails after a request changes data, the operation might have completed.
Check the affected path before you repeat the request.
A missing response does not mean that the operation failed.

Atomic replacement, shared locks, write order, and storage durability guarantees are not specified.
