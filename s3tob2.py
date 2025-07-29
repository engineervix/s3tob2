#!/usr/bin/env python3
"""
Script to transfer files from Amazon S3 to Backblaze B2
Requires: pip install boto3 b2sdk python-dotenv
"""

import os
import sys
import hashlib
import time
import logging
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from b2sdk.v2 import B2Api, InMemoryAccountInfo
from b2sdk.v2.exception import B2Error
from dotenv import load_dotenv


# Configuration
@dataclass
class TransferConfig:
    # S3 Configuration
    s3_bucket: str

    # B2 Configuration
    b2_bucket: str
    b2_application_key_id: str
    b2_application_key: str

    # S3 optional/defaults
    s3_prefix: str = ""  # Optional prefix to filter objects
    aws_access_key_id: Optional[str] = None  # If None, uses default AWS credentials
    aws_secret_access_key: Optional[str] = None
    aws_region: str = "us-east-1"

    # Transfer options
    delete_from_s3: bool = False  # Set to True to move instead of copy
    max_workers: int = 5  # Number of concurrent transfers
    verify_checksums: bool = True  # Verify file integrity after transfer
    skip_existing: bool = True  # Skip files that already exist in B2


class S3ToB2Transfer:
    def __init__(self, config: TransferConfig):
        self.config = config
        self.logger = self._setup_logging()

        # Initialize S3 client
        try:
            if config.aws_access_key_id and config.aws_secret_access_key:
                self.s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=config.aws_access_key_id,
                    aws_secret_access_key=config.aws_secret_access_key,
                    region_name=config.aws_region,
                )
            else:
                self.s3_client = boto3.client("s3", region_name=config.aws_region)
        except NoCredentialsError:
            self.logger.error("AWS credentials not found. Please configure AWS credentials.")
            sys.exit(1)

        # Initialize B2 API
        try:
            # Use InMemoryAccountInfo from the correct module
            info = InMemoryAccountInfo()
            self.b2_api = B2Api(account_info=info) # type: ignore
            self.b2_api.authorize_account("production", config.b2_application_key_id, config.b2_application_key)
            self.b2_bucket = self.b2_api.get_bucket_by_name(config.b2_bucket)
        except B2Error as e:
            self.logger.error(f"Failed to connect to B2: {e}")
            sys.exit(1)

    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler("s3_to_b2_transfer.log"), logging.StreamHandler(sys.stdout)],
        )
        return logging.getLogger(__name__)

    def list_s3_objects(self) -> List[dict]:
        """List all objects in the S3 bucket with the specified prefix"""
        objects = []
        paginator = self.s3_client.get_paginator("list_objects_v2")

        try:
            for page in paginator.paginate(Bucket=self.config.s3_bucket, Prefix=self.config.s3_prefix):
                if "Contents" in page:
                    objects.extend(page["Contents"])
        except ClientError as e:
            self.logger.error(f"Error listing S3 objects: {e}")
            return []

        self.logger.info(f"Found {len(objects)} objects in S3 bucket")
        return objects

    def file_exists_in_b2(self, key: str) -> bool:
        """Check if a file already exists in B2"""
        if not self.config.skip_existing:
            return False

        try:
            file_info = self.b2_bucket.get_file_info_by_name(key)
            return file_info is not None
        except B2Error:
            return False

    def calculate_md5(self, data: bytes) -> str:
        """Calculate MD5 hash of data"""
        return hashlib.md5(data).hexdigest()

    def transfer_file(self, s3_object: dict) -> bool:
        """Transfer a single file from S3 to B2"""
        key = s3_object["Key"]
        size = s3_object["Size"]

        try:
            # Check if file already exists in B2
            if self.file_exists_in_b2(key):
                self.logger.info(f"Skipping {key} (already exists in B2)")
                return True

            # Download from S3
            self.logger.info(f"Downloading {key} ({size} bytes) from S3...")
            response = self.s3_client.get_object(Bucket=self.config.s3_bucket, Key=key)
            file_data = response["Body"].read()

            # Get S3 ETag for verification (if available)
            s3_etag = response.get("ETag", "").strip('"')

            # Upload to B2
            self.logger.info(f"Uploading {key} to B2...")
            self.b2_bucket.upload_bytes(
                data_bytes=file_data,
                file_name=key,
                content_type=response.get("ContentType", "application/octet-stream"),
                file_info={"src": "s3", "s3_etag": s3_etag, "transferred_at": str(int(time.time()))},
            )

            # Verify checksum if enabled
            if self.config.verify_checksums and s3_etag:
                local_md5 = self.calculate_md5(file_data)
                if local_md5 != s3_etag:
                    self.logger.warning(f"Checksum mismatch for {key}")

            # Delete from S3 if moving
            if self.config.delete_from_s3:
                self.logger.info(f"Deleting {key} from S3...")
                self.s3_client.delete_object(Bucket=self.config.s3_bucket, Key=key)

            self.logger.info(f"Successfully transferred {key}")
            return True

        except (ClientError, B2Error) as e:
            self.logger.error(f"Failed to transfer {key}: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error transferring {key}: {e}")
            return False

    def transfer_all(self) -> None:
        """Transfer all files from S3 to B2"""
        objects = self.list_s3_objects()
        if not objects:
            self.logger.info("No objects found to transfer")
            return

        successful_transfers = 0
        failed_transfers = 0

        self.logger.info(f"Starting transfer of {len(objects)} objects...")

        # Use ThreadPoolExecutor for concurrent transfers
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit all transfer tasks
            future_to_key = {executor.submit(self.transfer_file, obj): obj["Key"] for obj in objects}

            # Process completed transfers
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    success = future.result()
                    if success:
                        successful_transfers += 1
                    else:
                        failed_transfers += 1
                except Exception as e:
                    self.logger.error(f"Transfer task for {key} raised exception: {e}")
                    failed_transfers += 1

        # Summary
        self.logger.info("=" * 50)
        self.logger.info("TRANSFER SUMMARY")
        self.logger.info("=" * 50)
        self.logger.info(f"Total objects: {len(objects)}")
        self.logger.info(f"Successful transfers: {successful_transfers}")
        self.logger.info(f"Failed transfers: {failed_transfers}")
        self.logger.info(f"Action: {'Move' if self.config.delete_from_s3 else 'Copy'}")


def load_config() -> TransferConfig:
    """Load configuration from environment variables"""
    # Load .env file
    load_dotenv()

    # Helper function to convert string to boolean
    def str_to_bool(value: str) -> bool:
        return value.lower() in ("true", "1", "yes", "on")

    # Load configuration from environment variables
    config = TransferConfig(
        # S3 settings
        s3_bucket=os.getenv("S3_BUCKET", ""),
        s3_prefix=os.getenv("S3_PREFIX", ""),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),  # Can be None for default credentials
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),  # Can be None for default credentials
        aws_region=os.getenv("AWS_REGION", "us-east-1"),
        # B2 settings
        b2_bucket=os.getenv("B2_BUCKET", ""),
        b2_application_key_id=os.getenv("B2_APPLICATION_KEY_ID", ""),
        b2_application_key=os.getenv("B2_APPLICATION_KEY", ""),
        # Transfer options
        delete_from_s3=str_to_bool(os.getenv("DELETE_FROM_S3", "false")),
        max_workers=int(os.getenv("MAX_WORKERS", "5")),
        verify_checksums=str_to_bool(os.getenv("VERIFY_CHECKSUMS", "true")),
        skip_existing=str_to_bool(os.getenv("SKIP_EXISTING", "true")),
    )

    return config


def validate_config(config: TransferConfig) -> bool:
    """Validate required configuration values"""
    required_fields = [
        ("S3_BUCKET", config.s3_bucket),
        ("B2_BUCKET", config.b2_bucket),
        ("B2_APPLICATION_KEY_ID", config.b2_application_key_id),
        ("B2_APPLICATION_KEY", config.b2_application_key),
    ]

    missing_fields = [field_name for field_name, value in required_fields if not value]

    if missing_fields:
        print("Error: Missing required environment variables:")
        for field in missing_fields:
            print(f"  - {field}")
        print("\nPlease check your .env file or environment variables.")
        return False

    return True


def main():
    # Load configuration from .env file
    config = load_config()

    # Validate configuration
    if not validate_config(config):
        sys.exit(1)

    # Display configuration (without sensitive data)
    print("Configuration loaded:")
    print(f"  S3 Bucket: {config.s3_bucket}")
    print(f"  S3 Prefix: '{config.s3_prefix}'")
    print(f"  AWS Region: {config.aws_region}")
    print(f"  B2 Bucket: {config.b2_bucket}")
    print(f"  Delete from S3: {config.delete_from_s3}")
    print(f"  Max Workers: {config.max_workers}")
    print(f"  Verify Checksums: {config.verify_checksums}")
    print(f"  Skip Existing: {config.skip_existing}")
    print()

    # Run transfer
    transfer = S3ToB2Transfer(config)
    transfer.transfer_all()


if __name__ == "__main__":
    main()
