import os
import zipfile
import pandas as pd
import logging
from google.cloud import storage
from google.cloud import bigquery
import argparse
import glob
import requests
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_safe(zip_path, extract_path):
    """Safely extracts zip file preventing Zip Slip vulnerability."""
    if not os.path.exists(extract_path):
        os.makedirs(extract_path)
        
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for member in zip_ref.infolist():
            if member.filename.startswith('/') or '..' in member.filename:
                logger.warning(f"Skipping dangerous path: {member.filename}")
                continue
            zip_ref.extract(member, extract_path)

def upload_blob(bucket, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    logger.info(f"Uploaded {source_file_name} to {destination_blob_name}.")

def parse_schema(sql_file_path):
    """
    Parses capublic.sql to extract table schemas.
    Returns a dict: {table_name: [column_names]}
    """
    schema_map = {}
    current_table = None
    
    with open(sql_file_path, 'r', encoding='latin1') as f:
        for line in f:
            line = line.strip()
            if line.upper().startswith("CREATE TABLE"):
                parts = line.split()
                if len(parts) >= 3:
                    raw_table = parts[2]
                    if '.' in raw_table:
                        raw_table = raw_table.split('.')[-1]
                    current_table = raw_table.strip('`"\'();').lower()
                    schema_map[current_table] = []
            elif current_table and line.startswith("`"):
                col_name = line.split()[0].strip('`"\'')
                schema_map[current_table].append(col_name)
            elif line.startswith(")") and current_table:
                current_table = None
                
    return schema_map

def load_to_bigquery(client, dataset_id, table_name, uri):
    """Loads a Parquet file from GCS Schema to BigQuery."""
    table_id = f"{client.project}.{dataset_id}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Replace daily
        autodetect=True, # Parquet has schema, so autodetect usually works well
    )

    try:
        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )
        load_job.result()  # Waits for the job to complete.
        logger.info(f"Loaded {uri} into {table_id}.")
        
        # Verify row count
        destination_table = client.get_table(table_id)
        logger.info(f"Loaded {destination_table.num_rows} rows.")
        
    except Exception as e:
        logger.error(f"Failed to load {table_name} to BigQuery: {e}")

def process_dat_file(dat_file, schema_map, bucket, bq_client, dataset_id, extract_path, limit=None):
    """
    Parses a .dat file, converts to Parquet, uploads to GCS, and loads to BigQuery.
    Handles LOB inlining for BILL_VERSION_TBL if corresponding .lob files exist.
    """
    table_name = os.path.basename(dat_file).replace('.dat', '').lower()
    logger.info(f"Processing table: {table_name}")
    
    if table_name not in schema_map:
        logger.warning(f"No schema found for {table_name}, skipping.")
        return

    columns = schema_map[table_name]
    
    try:
        # Check if we need to handle LOB inlining for bill_version_tbl
        is_bill_version = ('bill_version_tbl' in table_name)
        
        # Use a smaller chunk size for bills to avoid OOM when inlining LOBs
        chunk_size = 100 if is_bill_version else 100000
        
        chunk_iter = pd.read_csv(dat_file, sep='\t', header=None, names=columns, encoding='latin1', quoting=3, on_bad_lines='warn', chunksize=chunk_size, nrows=limit)
        
        gcs_uris = []
        part_num = 0
        import gc
        
        for chunk in chunk_iter:
            try:
                # LOB Inlining Logic
                if is_bill_version and 'bill_xml' in chunk.columns:
                    logger.info(f"Inlining LOBs for chunk {part_num} (Size: {len(chunk)})...")
                    
                    def read_lob(filename):
                        if not isinstance(filename, str) or not filename.endswith('.lob'):
                            return None
                        try:
                            # Files are usually in the same directory or root extract path
                            # Check local dir first (if extracted flat) OR check if it's a relative path?
                            # In capublic, they are often in the same folder as .dat
                            lob_path = os.path.join(os.path.dirname(dat_file), filename)
                            if not os.path.exists(lob_path):
                                 # Try root extract path
                                 lob_path = os.path.join(extract_path, filename)
                            
                            if os.path.exists(lob_path):
                                with open(lob_path, 'r', encoding='latin1', errors='replace') as f:
                                    return f.read()
                            return None # File not found
                        except Exception as e:
                            return None

                    # Apply to the column
                    chunk['bill_xml'] = chunk['bill_xml'].apply(read_lob)
                
                output_file = f"{table_name}_part_{part_num}.parquet"
                # Clean up string columns by stripping backticks and quotes
                # This fixes issues where 'QUOTE_NONE' (quoting=3) reads quotes into the data.
                for col in chunk.select_dtypes(include=['object']).columns:
                     chunk[col] = chunk[col].str.strip('`"\'')

                output_file = f"{table_name}_part_{part_num}.parquet"
                chunk.to_parquet(output_file, index=False)
                
                blob_name = f"staging/{table_name}/{output_file}"
                upload_blob(bucket, output_file, blob_name)
                gcs_uris.append(f"gs://{bucket.name}/{blob_name}")
                
            finally:
                if os.path.exists(output_file):
                    os.remove(output_file)
                part_num += 1
                # Aggressive cleanup
                del chunk
                gc.collect()
            
        logger.info(f"Processed {table_name} into {part_num} chunks.")
        
        if gcs_uris:
            # Load all parts into BigQuery
            wildcard_uri = f"gs://{bucket.name}/staging/{table_name}/*.parquet"
            logger.info(f"Loading {wildcard_uri} into BigQuery...")
            load_to_bigquery(bq_client, dataset_id, table_name, wildcard_uri)
            
    except Exception as e:
        logger.error(f"Failed to process {dat_file}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description='Ingest CA Legislative Data')
    parser.add_argument('--zip-url', help='URL to zip file', default="http://downloads.leginfo.legislature.ca.gov/pubinfo_2025.zip")
    parser.add_argument('--zip-file', help='Local path to zip file', required=False)
    parser.add_argument('--bucket', help='GCS Bucket for staging', default=os.environ.get("GCS_BUCKET"))
    parser.add_argument('--project', help='GCP Project ID', default=os.environ.get("GCP_PROJECT"))
    parser.add_argument('--dataset', help='BigQuery Dataset ID', default="cal_legislature_data")
    parser.add_argument('--limit', help='Limit rows for sampling', type=int, default=None)
    args = parser.parse_args()
    
    # Validate required arguments that no longer have fallbacks
    if not args.project:
        logger.error("GCP_PROJECT environment variable or --project argument is required.")
        return
    if not args.bucket:
        logger.error("GCS_BUCKET environment variable or --bucket argument is required.")
        return

    # Initialize Clients
    storage_client = storage.Client(project=args.project)
    bucket = storage_client.bucket(args.bucket)

    bq_client = bigquery.Client(project=args.project)

    zip_path = None
    extract_path = "extracted_data"
    
    if args.zip_file:
        if args.zip_file.startswith("gs://"):
            logger.info(f"Downloading zip from GCS: {args.zip_file}")
            zip_path = "downloaded.zip"
            try:
                # Parse gs://bucket/blob
                parts = args.zip_file.replace("gs://", "").split("/", 1)
                source_bucket_name = parts[0]
                source_blob_name = parts[1]
                
                source_bucket = storage_client.bucket(source_bucket_name)
                blob = source_bucket.blob(source_blob_name)
                blob.download_to_filename(zip_path)
                logger.info(f"Downloaded {args.zip_file} to {zip_path}")
            except Exception as e:
                logger.error(f"Failed to download zip from GCS: {e}")
                return
        else:
            logger.info(f"Using local zip: {args.zip_file}")
            zip_path = args.zip_file
    elif args.zip_url:
        logger.info(f"Downloading zip from: {args.zip_url}")
        zip_path = "downloaded.zip"
        try:
            with requests.get(args.zip_url, stream=True, timeout=(10, 300)) as r:
                r.raise_for_status()
                with open(zip_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
        except Exception as e:
            logger.error(f"Failed to download zip: {e}")
            return

    if zip_path and os.path.exists(zip_path):
        extract_safe(zip_path, extract_path)
            
        sql_path = os.path.join(extract_path, "capublic.sql")
        if not os.path.exists(sql_path):
            # Recursively search
             for root, dirs, files in os.walk(extract_path):
                 if "capublic.sql" in files:
                     sql_path = os.path.join(root, "capublic.sql")
                     break
        
        # Fallback to current dir (bundled)
        if not os.path.exists(sql_path) and os.path.exists("capublic.sql"):
             sql_path = "capublic.sql"
             logger.info("Using bundled capublic.sql fallback.")

        if os.path.exists(sql_path):
            schema_map = parse_schema(sql_path)
            
            dat_files = glob.glob(f"{extract_path}/**/*.dat", recursive=True)
            if not dat_files:
                # Some dumps put files in subfolders
                dat_files = glob.glob(f"{extract_path}/*/*.dat", recursive=True)

            logger.info(f"Found {len(dat_files)} .dat files to process.")
            
            for dat in dat_files:
                process_dat_file(dat, schema_map, bucket, bq_client, args.dataset, extract_path, limit=args.limit)
        else:
            logger.error("capublic.sql not found in zip or bundled.")
            
        # Cleanup
        if os.path.exists(extract_path):
            shutil.rmtree(extract_path)
        if os.path.exists("downloaded.zip"):
            os.remove("downloaded.zip")

if __name__ == "__main__":
    main()
