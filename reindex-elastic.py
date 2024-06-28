import warnings
from elasticsearch import Elasticsearch, helpers
from elastic_transport import ConnectionError as ESConnectionError, ApiError
from tqdm import tqdm
import urllib3
import time
import logging
import os
import concurrent.futures
from datetime import datetime
import argparse

# Set up logging to file
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'logs.txt')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Suppress the InsecureRequestWarning from urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Suppress the specific Elasticsearch SecurityWarning
warnings.filterwarnings('ignore', category=UserWarning, module='elasticsearch')

# Configuration for old Elasticsearch cluster
old_es_host = 'https://localhost:9200'
old_index = 'combolists-leaks'

# Authentication credentials
username = 'elastic'
password = 'changeme'

# Maximum number of documents per index
max_docs_per_index = 10000000

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Reindex Elasticsearch indices.')
parser.add_argument('--reindex-without-asking', action='store_true', help='Recreate indices without asking for confirmation.')
args = parser.parse_args()

# Helper function to connect to Elasticsearch with retries
def connect_es(host, basic_auth, max_retries=5, delay=5):
    for attempt in range(max_retries):
        try:
            es = Elasticsearch(host, basic_auth=basic_auth, verify_certs=False)
            if es.ping():
                logger.info(f"Successfully connected to {host}")
                return es
        except ESConnectionError as e:
            logger.error(f"Connection to {host} failed: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
    raise ConnectionError(f"Failed to connect to {host} after {max_retries} attempts")

# Connect to old Elasticsearch cluster with authentication and no certificate verification
old_es = connect_es(old_es_host, basic_auth=(username, password))

# Check if the old index exists
if not old_es.indices.exists(index=old_index):
    logger.error(f"Index '{old_index}' does not exist in the old Elasticsearch cluster.")
    exit(1)

# Get the old index settings and mappings
old_index_info = old_es.indices.get(index=old_index)[old_index]
old_index_settings = old_index_info['settings']['index']
old_index_mappings = old_index_info['mappings']

# Remove settings that are not allowed when creating a new index
disallowed_settings = ['creation_date', 'provided_name', 'uuid', 'version']
filtered_settings = {k: v for k, v in old_index_settings.items() if k not in disallowed_settings}

# Function to create a new index with retry and backoff
def create_new_index(new_es, base_index_name, index_suffix, reindex_without_asking, max_retries=5):
    new_index = f"{base_index_name}-{index_suffix}"
    for attempt in range(max_retries):
        try:
            if new_es.indices.exists(index=new_index):
                logger.warning(f"Index '{new_index}' already exists.")
                if not reindex_without_asking:
                    proceed = input(f"Index '{new_index}' already exists. Do you want to continue and overwrite it? (yes/no): ")
                    if proceed.lower() != 'yes':
                        logger.info("Operation cancelled by the user.")
                        exit(0)
                new_es.indices.delete(index=new_index)
                logger.info(f"Deleted existing index '{new_index}'.")
            new_es.indices.create(index=new_index, body={
                'settings': filtered_settings,
                'mappings': old_index_mappings
            })
            logger.info(f"Created new index '{new_index}'.")
            return new_index
        except ApiError as e:
            if e.status_code == 429:
                wait_time = 2 ** attempt
                logger.warning(f"Received 429 Too Many Requests. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                raise
    raise ApiError(f"Failed to create new index '{new_index}' after {max_retries} attempts")

# Function to reindex a chunk of data
def reindex_chunk(data_chunk, new_es, current_new_index):
    actions = [
        {
            '_op_type': 'index',
            '_index': current_new_index,
            '_id': doc['_id'],
            '_source': doc['_source']
        }
        for doc in data_chunk['hits']['hits']
    ]
    helpers.bulk(new_es, actions)
    return len(data_chunk['hits']['hits'])

# Connect to new Elasticsearch cluster
new_es_host = 'https://localhost:9200'
new_es = connect_es(new_es_host, basic_auth=(username, password))

# Generate the initial new index name
base_new_index = 'combolist-leaks'
date_suffix = datetime.now().strftime("%d-%m-%Y")
suffix_count = 1
current_index_suffix = f"{date_suffix}-{suffix_count:02d}"
current_new_index = create_new_index(new_es, base_new_index, current_index_suffix, args.reindex_without_asking)

# Get the total number of documents in the old index
total_docs = old_es.count(index=old_index)['count']

# Scroll through the old index and bulk index documents to the new index
scroll = '2m'
size = 1000
indexed_docs = 0

# Initialize the scroll
data = old_es.search(
    index=old_index,
    scroll=scroll,
    size=size,
    body={
        'query': {
            'match_all': {}
        }
    }
)

# Set up the progress bar
progress_bar = tqdm(total=total_docs, desc="Reindexing", unit="docs", dynamic_ncols=True)

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    futures = []
    while data['hits']['hits']:
        futures.append(executor.submit(reindex_chunk, data, new_es, current_new_index))
        
        # Get the next scroll
        scroll_id = data['_scroll_id']
        data = old_es.scroll(scroll_id=scroll_id, scroll=scroll)

        # Process completed futures
        for future in concurrent.futures.as_completed(futures):
            docs_indexed_in_chunk = future.result()
            indexed_docs += docs_indexed_in_chunk
            progress_bar.update(docs_indexed_in_chunk)
            futures.remove(future)
            
            # Check if the current new index has reached the maximum document count
            if indexed_docs >= max_docs_per_index:
                suffix_count += 1
                current_index_suffix = f"{date_suffix}-{suffix_count:02d}"
                current_new_index = create_new_index(new_es, base_new_index, current_index_suffix, args.reindex_without_asking)
                indexed_docs = 0

# Close the progress bar
progress_bar.close()

logger.info(f"Reindexing from '{old_index}' to new indices completed.")