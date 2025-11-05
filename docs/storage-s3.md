# S3-Compatible Blob Storage

Zetesis supports S3-compatible object storage as an alternative to the default filesystem-based blob storage. This enables using cloud storage services like Amazon S3, Hetzner Object Storage, MinIO, and other S3-compatible providers.

## Features

- **S3-compatible API**: Works with AWS S3 and S3-compatible services (Hetzner, MinIO, etc.)
- **Path-style addressing**: Full support for custom endpoints with path-style addressing
- **Key layout mirroring**: S3 object keys mirror the filesystem layout for consistency
- **Idempotent uploads**: Duplicate content is automatically detected and not re-uploaded
- **Streaming support**: Efficient streaming reads with configurable chunk sizes
- **Content-addressed storage**: BLAKE3 content identifiers ensure data integrity

## Building with S3 Support

S3 support is disabled by default. Enable it with the `s3` feature flag:

```bash
cargo build --features s3
```

Or in your `Cargo.toml`:

```toml
[dependencies]
zetesis-app = { version = "0.1", features = ["s3"] }
```

## Configuration

### Basic Configuration

Add the following to your `config/settings.toml`:

```toml
[storage]
backend = "s3"  # Use "fs" for filesystem (default)

[storage.s3]
bucket = "your-bucket-name"
endpoint_url = "https://nbg1.your-objectstorage.com"  # For Hetzner
region = "nbg1"  # Optional, defaults to AWS behavior chain
force_path_style = true  # Required for non-AWS S3 providers
root_prefix = ""  # Optional, prefix for all keys (e.g., "zetesis/")
```

### Environment Variables

You can also configure via environment variables:

```bash
export ZETESIS__STORAGE__BACKEND=s3
export ZETESIS__STORAGE__S3__BUCKET=your-bucket-name
export ZETESIS__STORAGE__S3__ENDPOINT_URL=https://nbg1.your-objectstorage.com
export ZETESIS__STORAGE__S3__REGION=nbg1
export ZETESIS__STORAGE__S3__FORCE_PATH_STYLE=true

# AWS credentials (required)
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
```

### Credentials

The S3 implementation uses the AWS SDK's default credential provider chain, which checks:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. AWS config file (`~/.aws/config`)
4. IAM instance profile (if running on EC2)

## Provider-Specific Configuration

### Hetzner Object Storage

Hetzner provides S3-compatible object storage in multiple regions:

```toml
[storage.s3]
bucket = "my-zetesis-bucket"
endpoint_url = "https://nbg1.your-objectstorage.com"  # Nuremberg
region = "nbg1"
force_path_style = true
```

Available regions:
- `nbg1` - Nuremberg, Germany: `https://nbg1.your-objectstorage.com`
- `fsn1` - Falkenstein, Germany: `https://fsn1.your-objectstorage.com`
- `hel1` - Helsinki, Finland: `https://hel1.your-objectstorage.com`

### AWS S3

For native AWS S3:

```toml
[storage.s3]
bucket = "my-zetesis-bucket"
region = "us-east-1"  # Or your preferred region
force_path_style = false  # AWS supports virtual-hosted-style
# endpoint_url not needed for AWS S3
```

### MinIO

For self-hosted MinIO:

```toml
[storage.s3]
bucket = "zetesis"
endpoint_url = "http://localhost:9000"
region = "us-east-1"  # MinIO default
force_path_style = true
```

## Object Key Structure

S3 object keys mirror the filesystem layout to maintain consistency:

```
blobs/{silo}/{cid[0:2]}/{cid}
```

Examples:
- `blobs/kio/ab/abcdef1234567890...` (kio silo)
- `blobs/core/12/123456789abcdef0...` (core silo)

With a `root_prefix` of `"zetesis/"`:
- `zetesis/blobs/kio/ab/abcdef1234567890...`

This sharding strategy (using the first 2 characters of the CID) ensures:
- Efficient directory listings
- Reduced impact of key prefix limitations
- Consistent behavior across storage backends

## Limitations

### Single-Part Upload Only (Current Implementation)

The current implementation uses single-part uploads with a default maximum size of 256 MiB. Attempting to upload larger blobs will result in an error:

```
BlobError::Io("data exceeds max single-part upload size of 268435456 bytes")
```

**Workaround**: For now, ensure individual blobs stay under 256 MiB. Future versions will support multipart uploads for larger objects.

### Memory Usage

During uploads, the entire blob is buffered in memory while computing the content identifier (CID). For large files, this may impact memory usage.

## Testing

### Unit Tests

Run unit tests (no S3 connection required):

```bash
cargo test --features s3
```

### Live Integration Test

To test against actual S3-compatible storage, set environment variables and run the ignored test:

```bash
export HZ_S3_BUCKET=test-bucket
export HZ_S3_ENDPOINT=https://nbg1.your-objectstorage.com
export HZ_S3_REGION=nbg1
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret

cargo test --features s3 s3_live_integration_test -- --ignored --nocapture
```

This test performs a full roundtrip: put, get, head, delete operations.

## Migration from Filesystem Storage

To migrate existing filesystem blob storage to S3:

1. **Backup your data** (always!)
2. **Configure S3** as shown above
3. **Copy blobs** using a sync tool:
   - The S3 key structure mirrors the filesystem layout
   - Use `aws s3 sync` or `rclone` to copy:

```bash
# Using AWS CLI
aws s3 sync ~/.local/share/zetesis/blobs/ s3://your-bucket/blobs/ \
    --endpoint-url https://nbg1.your-objectstorage.com

# Using rclone
rclone sync ~/.local/share/zetesis/blobs/ hetzner:your-bucket/blobs/
```

4. **Update configuration** to use S3 backend
5. **Verify** blob retrieval works correctly

## Troubleshooting

### SignatureDoesNotMatch Error

If you see signature errors:
- Verify `force_path_style = true` for non-AWS providers
- Check that `endpoint_url` includes the protocol (`https://`)
- Ensure credentials are correct
- Verify the region matches your storage provider

### Connection Timeout

- Check firewall rules and network connectivity
- Verify the endpoint URL is correct
- Ensure the bucket exists and is accessible

### Access Denied

- Verify AWS credentials are correct
- Check bucket permissions (must allow PutObject, GetObject, HeadObject, DeleteObject)
- For Hetzner: ensure the access key has appropriate permissions

## Performance Considerations

- **Latency**: S3 operations have higher latency than local filesystem
- **Bandwidth**: Large blobs benefit from being closer to the S3 endpoint
- **Costs**: Be aware of storage, request, and egress costs for your provider
- **Concurrency**: The implementation is thread-safe and supports concurrent operations

## Future Enhancements

Planned improvements for S3 support:
- Multipart upload for blobs > 256 MiB
- Streaming uploads to reduce memory usage
- Configurable retry policies and timeouts
- Server-side encryption (SSE) support
- Lifecycle policies and bucket versioning
