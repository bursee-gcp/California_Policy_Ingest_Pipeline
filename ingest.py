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
import datetime
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def map_mysql_to_bq(mysql_type):
    """Maps MySQL types from capublic.sql to BigQuery types."""
    # Strip modifiers like BINARY, UNSIGNED, etc.
    mysql_type = mysql_type.upper().split()[0]  # Get first word (e.g., VARCHAR(20))
    mysql_type = mysql_type.split('(')[0]      # Get base type (e.g., VARCHAR)
    
    if mysql_type in ['VARCHAR', 'CHAR', 'LONGTEXT', 'TEXT', 'ENUM']:
        return 'STRING'
    elif mysql_type in ['INT', 'INTEGER', 'SMALLINT', 'TINYINT', 'MEDIUMINT', 'BIGINT']:
        return 'INTEGER'
    elif mysql_type in ['DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE']:
        return 'NUMERIC'
    elif mysql_type == 'DATETIME':
        return 'DATETIME'
    elif mysql_type == 'DATE':
        return 'DATE'
    elif mysql_type in ['LONGBLOB', 'BLOB', 'MEDIUMBLOB']:
        return 'BYTES'
    else:
        return 'STRING'

def parse_schema(sql_file_path):
    """
    Parses capublic.sql to extract table schemas with types.
    Returns a dict: {table_name: [bigquery.SchemaField]}
    """
    schema_map = {}
    current_table = None
    
    # Regex to match column definition: `name` TYPE [modifiers]
    col_pattern = re.compile(r'^`([^`]+)`\s+([A-Za-z0-9]+(?:\([^)]+\))?).*')
    
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
                match = col_pattern.match(line)
                if match:
                    col_name = match.group(1)
                    col_type = match.group(2)
                    bq_type = map_mysql_to_bq(col_type)
                    schema_map[current_table].append(
                        bigquery.SchemaField(col_name, bq_type)
                    )
            elif line.startswith(")") and current_table:
                current_table = None
                
    return schema_map

def upload_blob(bucket, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    logger.info(f"Uploaded {source_file_name} to {destination_blob_name}.")

def load_to_bigquery(client, dataset_id, table_name, uri, schema):
    """Loads a Parquet file from GCS into BigQuery with explicit schema."""
    table_id = f"{client.project}.{dataset_id}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    try:
        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )
        load_job.result()
        logger.info(f"Loaded {uri} into {table_id}.")
        
        destination_table = client.get_table(table_id)
        logger.info(f"Loaded {destination_table.num_rows} rows.")
        
    except Exception as e:
        logger.error(f"Failed to load {table_name} to BigQuery: {e}")

def process_dat_file(dat_file, table_name, schema, bucket, bq_client, dataset_id, zip_ref, limit=None):
    """
    Parses a .dat file, converts to Parquet, uploads to GCS, and loads to BigQuery.
    Streams processing chunk by chunk to prevent OOM.
    """
    logger.info(f"Processing table: {table_name}")
    columns = [field.name for field in schema]
    
    try:
        is_bill_version = ('bill_version_tbl' in table_name)
        chunk_size = 100 if is_bill_version else 100000
        
        chunk_iter = pd.read_csv(dat_file, sep='\t', header=None, names=columns, encoding='latin1', quoting=3, on_bad_lines='warn', chunksize=chunk_size, nrows=limit)
        
        gcs_uris = []
        part_num = 0
        import gc
        
        for chunk in chunk_iter:
            try:
                # LOB Inlining Logic for bill_version_tbl
                if is_bill_version and 'bill_xml' in chunk.columns:
                    logger.info(f"Inlining LOBs for chunk {part_num}...")
                    
                    def read_lob(filename):
                        if not isinstance(filename, str) or not filename.endswith('.lob'):
                            return None
                        try:
                            # Read directly from ZipFile stream
                            with zip_ref.open(filename) as f:
                                return f.read().decode('latin1', errors='replace')
                        except KeyError:
                            return None
                        except Exception:
                            return None

                    chunk['bill_xml'] = chunk['bill_xml'].apply(read_lob)
                
                # Cleanup and Type Management
                for col in chunk.select_dtypes(include=['object']).columns:
                     chunk[col] = chunk[col].astype(str).str.strip('`"\'')
                
                # Explicit conversion for BYTES to prevent type mismatch in Parquet
                for field in schema:
                    if field.name in chunk.columns and field.field_type == 'BYTES':
                        chunk[field.name] = chunk[field.name].apply(lambda x: x.encode('utf-8') if isinstance(x, str) else x)

                output_file = f"/tmp/{table_name}_part_{part_num}.parquet"
                chunk.to_parquet(output_file, index=False)
                
                blob_name = f"staging/{table_name}/{table_name}_part_{part_num}.parquet"
                upload_blob(bucket, output_file, blob_name)
                gcs_uris.append(f"gs://{bucket.name}/{blob_name}")
                
            finally:
                if os.path.exists(output_file):
                    os.remove(output_file)
                part_num += 1
                del chunk
                gc.collect()
            
        logger.info(f"Processed {table_name} into {part_num} chunks.")
        
        if gcs_uris:
            wildcard_uri = f"gs://{bucket.name}/staging/{table_name}/*.parquet"
            logger.info(f"Loading {wildcard_uri} into BigQuery...")
            load_to_bigquery(bq_client, dataset_id, table_name, wildcard_uri, schema)
            
    except Exception as e:
        logger.error(f"Failed to process {dat_file}: {e}")

def main():
    parser = argparse.ArgumentParser(description='Ingest CA Legislative Data')
    
    # Dynamic Year Calculation
    current_year = datetime.date.today().year
    default_url = f"http://downloads.leginfo.legislature.ca.gov/pubinfo_{current_year}.zip"
    
    parser.add_argument('--zip-url', help='URL to zip file', default=default_url)
    parser.add_argument('--zip-file', help='Local path to zip file', required=False)
    parser.add_argument('--bucket', help='GCS Bucket for staging', default=os.environ.get("GCS_BUCKET"))
    parser.add_argument('--project', help='GCP Project ID', default=os.environ.get("GCP_PROJECT"))
    parser.add_argument('--dataset', help='BigQuery Dataset ID', default="cal_legislature_data")
    parser.add_argument('--limit', help='Limit rows for sampling', type=int, default=None)
    args = parser.parse_args()
    
    if not args.project:
        logger.error("GCP_PROJECT environment variable or --project argument is required.")
        return
    if not args.bucket:
        logger.error("GCS_BUCKET environment variable or --bucket argument is required.")
        return

    storage_client = storage.Client(project=args.project)
    bucket = storage_client.bucket(args.bucket)
    bq_client = bigquery.Client(project=args.project)

    zip_path = "/tmp/downloaded.zip"
    
    # 1. Acquire the ZIP File
    if args.zip_file:
        if args.zip_file.startswith("gs://"):
            logger.info(f"Downloading zip from GCS: {args.zip_file}")
            try:
                parts = args.zip_file.replace("gs://", "").split("/", 1)
                source_bucket = storage_client.bucket(parts[0])
                blob = source_bucket.blob(parts[1])
                blob.download_to_filename(zip_path)
            except Exception as e:
                logger.error(f"Failed to download zip from GCS: {e}")
                return
        else:
            zip_path = args.zip_file
    elif args.zip_url:
        logger.info(f"Downloading zip from: {args.zip_url}")
        try:
            with requests.get(args.zip_url, stream=True, timeout=(10, 300)) as r:
                r.raise_for_status()
                with open(zip_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
        except Exception as e:
            logger.error(f"Failed to download zip: {e}")
            return

    # 2. Iterative Processing
    if os.path.exists(zip_path):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Extract capublic.sql to parse schema
            sql_path = None
            for name in zip_ref.namelist():
                if name.endswith("capublic.sql"):
                    logger.info(f"Extracting schema: {name}")
                    zip_ref.extract(name, "/tmp")
                    sql_path = f"/tmp/{name}"
                    break
                    
            if not sql_path:
                # Fallback to local file if it exists in container
                if os.path.exists("capublic.sql"):
                    sql_path = "capublic.sql"
                    logger.info("Using bundled capublic.sql fallback.")
                else:
                    logger.error("capublic.sql not found in ZIP or container.")
                    return

            schema_map = parse_schema(sql_path)
            logger.info(f"Parsed schemas for {len(schema_map)} tables.")

            # Process .dat files one by one to save space
            for member in zip_ref.infolist():
                if member.filename.endswith(".dat"):
                    table_name = os.path.basename(member.filename).replace('.dat', '').lower()
                    
                    if table_name not in schema_map:
                        logger.warning(f"No schema found for {table_name}, skipping.")
                        continue
                        
                    logger.info(f"Extracting {member.filename}...")
                    zip_ref.extract(member, "/tmp")
                    extracted_dat_path = f"/tmp/{member.filename}"
                    
                    process_dat_file(
                        extracted_dat_path, 
                        table_name, 
                        schema_map[table_name], 
                        bucket, 
                        bq_client, 
                        args.dataset, 
                        zip_ref, 
                        limit=args.limit
                    )
                    
                    if os.path.exists(extracted_dat_path):
                        os.remove(extracted_dat_path)

        # Cleanup
        if args.zip_file and not args.zip_file.startswith("gs://"):
            pass # Keep local file if passed as arg
        elif os.path.exists(zip_path):
            os.remove(zip_path)
            
        # Cleanup extracted SQL if it was from ZIP
        if sql_path.startswith("/tmp") and os.path.exists(sql_path):
            os.remove(sql_path)

if __name__ == "__main__":
    main()
