#!/usr/bin/env python3
"""
Credentials Setup CLI - Encrypt and store database credentials.

Usage:
    python -m assemblyline_common.credentials.setup

This will prompt for:
1. Database connection details (host, port, user, password, database)
2. Master password to encrypt the credentials

The encrypted credentials are stored in ~/.logic-weaver/credentials.enc
"""

import getpass
import sys
from pathlib import Path

from .master_credentials import (
    encrypt_credentials,
    decrypt_credentials,
    save_credentials,
    load_credentials,
    credentials_exist,
    DEFAULT_CREDENTIALS_FILE,
)


def build_database_url(connection_type: str) -> str:
    """Build a database URL from user input for a specific connection type."""
    if connection_type == "aws":
        print("\n--- AWS RDS Connection ---")
        host = input("RDS Endpoint (e.g., mydb.xxx.us-east-1.rds.amazonaws.com): ").strip()
        port = input("Port [5432]: ").strip() or "5432"
        default_ssl = "require"
    else:
        print("\n--- Local PostgreSQL Connection ---")
        host = input("Host [localhost]: ").strip() or "localhost"
        port = input("Port [5432]: ").strip() or "5432"
        default_ssl = ""

    database = input("Database name [message_weaver]: ").strip() or "message_weaver"
    user = input("Username [postgres]: ").strip() or "postgres"
    password = getpass.getpass("Password: ")

    # SSL options
    if connection_type == "aws":
        ssl = input(f"SSL mode [{default_ssl}]: ").strip() or default_ssl
    else:
        ssl = input("SSL mode (leave empty for none, or 'require'): ").strip()

    # Build URL - Using asyncpg driver for SQLAlchemy async
    base_url = f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}"

    if ssl:
        base_url += f"?ssl={ssl}"

    return base_url


def prompt_database_urls() -> dict:
    """Prompt user to build database URLs for both local and AWS environments."""
    print("\n=== Database Connection Setup ===\n")
    print("This setup will configure BOTH local and AWS RDS connections.")
    print("You can switch between them using the DB_ENVIRONMENT setting.\n")

    urls = {}

    # Setup Local PostgreSQL
    print("=" * 40)
    print("Step 1: Local PostgreSQL Configuration")
    print("=" * 40)
    skip_local = input("Skip local setup? (y/N): ").strip().lower()
    if skip_local != "y":
        urls["local_database_url"] = build_database_url("local")
    else:
        print("Skipped local configuration.")

    # Setup AWS RDS
    print("\n" + "=" * 40)
    print("Step 2: AWS RDS Configuration")
    print("=" * 40)
    skip_aws = input("Skip AWS RDS setup? (y/N): ").strip().lower()
    if skip_aws != "y":
        urls["aws_database_url"] = build_database_url("aws")
    else:
        print("Skipped AWS configuration.")

    if not urls:
        print("\nNo database URLs configured!")
        manual = input("Enter a single DATABASE_URL manually? (y/N): ").strip().lower()
        if manual == "y":
            urls["database_url"] = getpass.getpass("Enter full DATABASE_URL: ")

    return urls


def prompt_master_password(confirm: bool = True) -> str:
    """Prompt user for master password."""
    print("\n=== Master Password ===")
    print("This password encrypts your database credentials.")
    print("You'll need to enter it each time the application starts.\n")

    while True:
        password = getpass.getpass("Enter master password: ")

        if len(password) < 8:
            print("Password must be at least 8 characters. Try again.")
            continue

        if confirm:
            confirm_password = getpass.getpass("Confirm master password: ")
            if password != confirm_password:
                print("Passwords don't match. Try again.")
                continue

        return password


def setup_credentials():
    """Main setup function."""
    print("\n" + "=" * 60)
    print("  Logic Weaver - Encrypted Credentials Setup")
    print("=" * 60)

    # Check if credentials already exist
    if credentials_exist():
        print(f"\nEncrypted credentials already exist at:")
        print(f"  {DEFAULT_CREDENTIALS_FILE}")
        choice = input("\nOverwrite existing credentials? (y/N): ").strip().lower()
        if choice != "y":
            print("Setup cancelled.")
            return

    # Get database URLs (both local and AWS)
    credentials = prompt_database_urls()

    if not credentials:
        print("\nNo credentials to save. Setup cancelled.")
        return

    # Get master password
    master_password = prompt_master_password(confirm=True)

    print("\nEncrypting credentials...")
    encrypted = encrypt_credentials(credentials, master_password)

    filepath = save_credentials(encrypted)
    print(f"\n✓ Credentials encrypted and saved to:")
    print(f"  {filepath}")

    # Verify by decrypting
    print("\nVerifying encryption...")
    try:
        decrypted = decrypt_credentials(encrypted, master_password)
        verified = True
        for key, value in credentials.items():
            if decrypted.get(key) != value:
                verified = False
                break
        if verified:
            print("✓ Verification successful!")
        else:
            print("✗ Verification failed - credentials mismatch!")
            sys.exit(1)
    except Exception as e:
        print(f"✗ Verification failed: {e}")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("  Setup Complete!")
    print("=" * 60)
    print("\nConfigured environments:")
    if "local_database_url" in credentials:
        print("  ✓ Local PostgreSQL")
    if "aws_database_url" in credentials:
        print("  ✓ AWS RDS")
    print("\nTo use encrypted credentials, set in your .env:")
    print("  USE_ENCRYPTED_CREDENTIALS=true")
    print("\nTo switch environments, set DB_ENVIRONMENT in .env:")
    print("  DB_ENVIRONMENT=local   # for local PostgreSQL")
    print("  DB_ENVIRONMENT=aws     # for AWS RDS")
    print("\nThe application will prompt for your master password on startup.")


def verify_credentials():
    """Verify existing credentials with master password."""
    if not credentials_exist():
        print("No encrypted credentials found.")
        print(f"Run setup first: python -m assemblyline_common.credentials.setup")
        sys.exit(1)

    print("\n=== Verify Encrypted Credentials ===\n")
    master_password = getpass.getpass("Enter master password: ")

    try:
        encrypted = load_credentials()
        credentials = decrypt_credentials(encrypted, master_password)
        print("\n✓ Credentials decrypted successfully!")
        print(f"\nConfigured environments:")

        if "local_database_url" in credentials:
            url = credentials["local_database_url"]
            # Extract host from URL for display
            if "@" in url:
                host = url.split("@")[1].split(":")[0].split("/")[0]
            else:
                host = "unknown"
            print(f"  ✓ Local PostgreSQL: {host}")

        if "aws_database_url" in credentials:
            url = credentials["aws_database_url"]
            if "@" in url:
                host = url.split("@")[1].split(":")[0].split("/")[0]
            else:
                host = "unknown"
            print(f"  ✓ AWS RDS: {host}")

        if "database_url" in credentials and "local_database_url" not in credentials:
            print(f"  ✓ Legacy single database URL configured")

        print("\nTo switch environments, set DB_ENVIRONMENT in .env:")
        print("  DB_ENVIRONMENT=local   # for local PostgreSQL")
        print("  DB_ENVIRONMENT=aws     # for AWS RDS")

    except ValueError as e:
        print(f"\n✗ Failed to decrypt: {e}")
        sys.exit(1)


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Logic Weaver Credentials Manager"
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify existing encrypted credentials",
    )
    parser.add_argument(
        "--path",
        type=Path,
        help=f"Custom credentials file path (default: {DEFAULT_CREDENTIALS_FILE})",
    )

    args = parser.parse_args()

    if args.verify:
        verify_credentials()
    else:
        setup_credentials()


if __name__ == "__main__":
    main()
