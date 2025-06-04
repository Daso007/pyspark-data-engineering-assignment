import pandas as pd
import boto3 # AWS SDK for Python
import time
import os
import uuid # For more unique filenames

# --- Configuration ---
# Path to  local large transactions CSV file (downloaded from GDrive)
TRANSACTIONS_FILE_PATH = 'transactions.csv' 

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = 'YOUR_AWS_ACCESS_KEY_ID_PLACEHOLDER'
AWS_SECRET_ACCESS_KEY = 'YOUR_AWS_SECRET_KEY_PLACEHOLDER'
AWS_REGION = 'ap-south-1' 
S3_BUCKET_NAME = 'daso-de-assignment-transaction-chunks'
# Processing Configuration
CHUNK_SIZE = 10000  # Number of transactions per chunk file
UPLOAD_INTERVAL_SECONDS = 1 # Time to wait between uploading chunks

# --- S3 Client Initialization ---
# If AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set above, boto3 will use them.
# If they are None or empty, boto3 will try other methods (like shared credentials file).
s3_client = None
try:
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION 
        )
    else:
        # This assumes credentials are configured via `aws configure` or environment variables
        s3_client = boto3.client('s3', region_name=AWS_REGION)
    print(f"Successfully initialized S3 client for region {AWS_REGION}.")
except Exception as e:
    print(f"Error initializing S3 client: {e}")
    exit() # Exit if S3 client can't be initialized

def upload_chunk_to_s3(local_file_path, s3_object_key):
    """
    Uploads a local file to the specified S3 bucket and object key.
    """
    if not s3_client:
        print("S3 client not initialized. Cannot upload.")
        return False
    try:
        s3_client.upload_file(local_file_path, S3_BUCKET_NAME, s3_object_key)
        print(f"Successfully uploaded {s3_object_key} to S3 bucket '{S3_BUCKET_NAME}'.")
        return True
    except FileNotFoundError:
        print(f"Error: Local file {local_file_path} not found for upload.")
        return False
    except Exception as e: # Catching boto3 specific exceptions like NoCredentialsError is better
        print(f"Error uploading {s3_object_key} to S3: {e}")
        return False

if __name__ == "__main__":
    print("--- Mechanism X: Transaction Chunk Feeder Starting ---")

    if not os.path.exists(TRANSACTIONS_FILE_PATH):
        print(f"ERROR: Source transactions file not found at '{TRANSACTIONS_FILE_PATH}'. Please check the path.")
        exit()

    chunk_number = 0
    processed_rows_total = 0

    try:
        # pd.read_csv can read the file in chunks
        # This simulates getting the "next 10,000 entries"
        for chunk_df in pd.read_csv(TRANSACTIONS_FILE_PATH, chunksize=CHUNK_SIZE):
            chunk_number += 1
            processed_rows_in_chunk = len(chunk_df)
            processed_rows_total += processed_rows_in_chunk

            print(f"\nProcessing Chunk #{chunk_number} with {processed_rows_in_chunk} transactions...")

            # Generate a unique name for the chunk file
            # Using timestamp and a part of UUID to ensure high uniqueness
            timestamp_str = time.strftime('%Y%m%d_%H%M%S')
            unique_id = str(uuid.uuid4()).split('-')[0] # first part of uuid
            chunk_file_name_s3 = f"transactions_chunk_{timestamp_str}_{unique_id}_part{chunk_number}.csv"
            
            # Define a local temporary path for the chunk file
            local_temp_chunk_file_path = f"temp_chunk_{chunk_number}.csv"

            # Save the chunk DataFrame to a local temporary CSV file
            # We include the header in each chunk file for Mechanism Y to read easily
            chunk_df.to_csv(local_temp_chunk_file_path, index=False, header=True)
            print(f"Saved chunk to local temporary file: {local_temp_chunk_file_path}")

            # Upload the local temporary CSV file to S3
            if upload_chunk_to_s3(local_temp_chunk_file_path, chunk_file_name_s3):
                # Optional: Clean up the local temporary file after successful upload
                try:
                    os.remove(local_temp_chunk_file_path)
                    print(f"Removed local temporary file: {local_temp_chunk_file_path}")
                except OSError as e:
                    print(f"Error removing local temporary file {local_temp_chunk_file_path}: {e}")
            else:
                print(f"Upload failed for chunk {chunk_number}. Moving to next chunk if any.")
                # Decide on error handling: stop, or log and continue? Here we continue.

            print(f"Total rows processed so far: {processed_rows_total}")
            
            # Wait for the specified interval before processing the next chunk
            print(f"Waiting for {UPLOAD_INTERVAL_SECONDS} second(s) before next chunk...")
            time.sleep(UPLOAD_INTERVAL_SECONDS)

        print("\n--- Mechanism X: All chunks from the source file have been processed and uploaded. ---")

    except FileNotFoundError:
        print(f"ERROR: The main transactions CSV file '{TRANSACTIONS_FILE_PATH}' was not found during processing.")
    except pd.errors.EmptyDataError:
        print(f"ERROR: The main transactions CSV file '{TRANSACTIONS_FILE_PATH}' is empty.")
    except Exception as e:
        print(f"An unexpected error occurred in Mechanism X: {e}")
    finally:
        print("--- Mechanism X: Shutting down. ---")