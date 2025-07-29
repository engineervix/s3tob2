# S3 to B2 Transfer Script

[![CI](https://github.com/engineervix/s3tob2/actions/workflows/main.yml/badge.svg)](https://github.com/engineervix/s3tob2/actions/workflows/main.yml)

A Python script to efficiently transfer files from Amazon S3 to Backblaze B2 storage with support for concurrent transfers, checksum verification, and resume capability.

## Features

- **Concurrent transfers** - Transfer multiple files in parallel for faster migration
- **Resume support** - Skip files that already exist in B2 (configurable)
- **Checksum verification** - Verify file integrity after transfer
- **Move or copy** - Option to delete files from S3 after successful transfer
- **Prefix filtering** - Transfer only files matching a specific prefix
- **Progress logging** - Detailed logs with transfer progress and summary
- **Error handling** - Graceful handling of transfer failures with detailed error reporting

## Requirements

- Python 3.10+
- Required packages:
   - boto3
   - b2sdk
   - python-dotenv

  ```bash
  pip install -r requirements.txt
  ```

## Configuration

Create a `.env` file in the same directory as the script with the following variables:

```env
# Required S3 settings
S3_BUCKET=your-s3-bucket-name

# Required B2 settings
B2_BUCKET=your-b2-bucket-name
B2_APPLICATION_KEY_ID=your-b2-key-id
B2_APPLICATION_KEY=your-b2-application-key

# Optional S3 settings
S3_PREFIX=path/to/files/              # Filter objects by prefix (default: "")
AWS_ACCESS_KEY_ID=your-aws-key        # Uses default AWS credentials if not set
AWS_SECRET_ACCESS_KEY=your-aws-secret  # Uses default AWS credentials if not set
AWS_REGION=us-east-1                   # AWS region (default: us-east-1)

# Optional transfer settings
DELETE_FROM_S3=false                   # Delete from S3 after transfer (default: false)
MAX_WORKERS=5                          # Number of concurrent transfers (default: 5)
VERIFY_CHECKSUMS=true                  # Verify file integrity (default: true)
SKIP_EXISTING=true                     # Skip files that exist in B2 (default: true)
```

## Usage

1. Set up your `.env` file with the required credentials
2. Run the script:
   ```bash
   python s3tob2.py
   ```

The script will:
- List all objects in the S3 bucket (with optional prefix filter)
- Transfer each file to B2, skipping existing files if configured
- Log progress to both console and `s3_to_b2_transfer.log`
- Display a summary of successful and failed transfers

## AWS Credentials

The script supports two methods for AWS authentication:

1. **Explicit credentials** - Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `.env`
2. **Default credentials** - Leave AWS credentials empty to use:
   - IAM instance role (when running on EC2)
   - AWS credentials file (`~/.aws/credentials`)
   - Environment variables

## B2 Setup

1. Create a B2 application key with the following capabilities:
   - `listBuckets`
   - `listFiles`
   - `readFiles`
   - `writeFiles`
   - `deleteFiles` (only needed if `DELETE_FROM_S3=true`)

2. Create or use an existing B2 bucket for the destination

## Transfer Options

### Copy vs Move
- **Copy mode** (`DELETE_FROM_S3=false`) - Files remain in S3 after transfer
- **Move mode** (`DELETE_FROM_S3=true`) - Files are deleted from S3 after successful transfer

### Resume Capability
When `SKIP_EXISTING=true`, the script checks if each file already exists in B2 before transferring. This allows you to resume interrupted transfers without re-uploading files.

### Concurrent Transfers
Adjust `MAX_WORKERS` based on your network capacity and file sizes. Higher values may improve throughput for many small files, while lower values may be better for large files.

## Logging

The script creates detailed logs in `s3_to_b2_transfer.log` including:
- File transfer progress
- Error messages for failed transfers
- Transfer summary with success/failure counts

## Error Handling

The script continues transferring remaining files even if individual transfers fail. Failed transfers are logged and counted in the final summary.

## Example Output

```
Configuration loaded:
  S3 Bucket: my-s3-bucket
  S3 Prefix: 'uploads/2024/'
  AWS Region: us-east-1
  B2 Bucket: my-b2-bucket
  Delete from S3: False
  Max Workers: 5
  Verify Checksums: True
  Skip Existing: True

2024-01-15 10:30:45 - INFO - Found 150 objects in S3 bucket
2024-01-15 10:30:45 - INFO - Starting transfer of 150 objects...
2024-01-15 10:30:46 - INFO - Downloading uploads/2024/file1.jpg (1048576 bytes) from S3...
2024-01-15 10:30:47 - INFO - Uploading uploads/2024/file1.jpg to B2...
2024-01-15 10:30:48 - INFO - Successfully transferred uploads/2024/file1.jpg
...
==================================================
TRANSFER SUMMARY
==================================================
Total objects: 150
Successful transfers: 148
Failed transfers: 2
Action: Copy
```

## Troubleshooting

### Authentication Errors
- Verify your AWS and B2 credentials are correct
- Ensure your B2 application key has the required capabilities
- Check that the bucket names exist and are accessible

### Transfer Failures
- Check the log file for detailed error messages
- Verify network connectivity to both S3 and B2
- Ensure sufficient permissions on both source and destination

### Performance Issues
- Adjust `MAX_WORKERS` based on your network and file characteristics
- For very large files, consider reducing concurrent transfers
- Monitor memory usage for large file transfers
