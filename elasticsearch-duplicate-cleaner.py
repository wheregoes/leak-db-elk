import warnings
from elasticsearch import Elasticsearch
import urllib3
import os
from datetime import datetime
import argparse
import time
from tqdm import tqdm
from collections import defaultdict

# Suppress the InsecureRequestWarning from urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Suppress the specific Elasticsearch SecurityWarning
warnings.filterwarnings('ignore', category=UserWarning, module='elasticsearch')

# Ensure the Elasticsearch host is correctly set to your server's IP and port
ELASTICSEARCH_HOSTS = ['http://localhost:9200']  # Change to 'https://' if using HTTPS
ELASTICSEARCH_AUTH = ('elastic', 'changeme')
LOGS_DIR = 'logs'
TEMP_FILES_DIR = 'temp_files'
RETRY_LIMIT = 3
RETRY_DELAY = 5  # seconds
INDEXES_FILE = 'indexes-to-check.txt'
SCROLL_TIMEOUT = '5m'
SCROLL_SIZE = 1000  # Set the scroll size to an appropriate value (<= 10000)

def log_message(message, log_file_path='script.log', level='info'):
    log_levels = {'info': 'INFO', 'warning': 'WARNING', 'error': 'ERROR'}
    log_level = log_levels.get(level.lower(), 'INFO')
    timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z')
    
    log_dir = os.path.join(LOGS_DIR)
    os.makedirs(log_dir, exist_ok=True)

    with open(os.path.join(log_dir, log_file_path), 'a') as log_file:
        log_file.write(f"{timestamp} - {log_level} - {message}\n")
        log_file.flush()

def fetch_all_hashes(es, index_name, scroll_size=SCROLL_SIZE, scroll_timeout=SCROLL_TIMEOUT):
    attempt = 0
    total_fetched = 0
    temp_file_path = os.path.join(TEMP_FILES_DIR, f"{index_name}-hashes.txt")
    os.makedirs(TEMP_FILES_DIR, exist_ok=True)
    
    with open(temp_file_path, 'w') as temp_file:
        while attempt < RETRY_LIMIT:
            try:
                response = es.search(index=index_name, body={
                    '_source': ['hash'],
                    'query': {
                        'match_all': {}
                    }
                }, scroll=scroll_timeout, size=scroll_size)

                scroll_id = response['_scroll_id']
                hits = response['hits']['hits']

                while hits:
                    batch_size = len(hits)
                    for hit in hits:
                        hash_value = hit['_source']['hash']
                        doc_id = hit['_id']
                        temp_file.write(f"{hash_value},{doc_id}\n")
                    
                    total_fetched += batch_size
                    log_message(f"Fetched {batch_size} documents from {index_name}. Total fetched: {total_fetched}")
                    response = es.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
                    scroll_id = response['_scroll_id']
                    hits = response['hits']['hits']

                es.clear_scroll(scroll_id=scroll_id)
                log_message(f"Fetched all hashes from index {index_name}", level='info')
                return temp_file_path
            except Exception as e:
                attempt += 1
                log_message(f"Error fetching hashes (Attempt {attempt}): {e}", 'error.log', level='error')
                if attempt < RETRY_LIMIT:
                    time.sleep(RETRY_DELAY)
                else:
                    return None

def sort_and_deduplicate_file(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    sorted_unique_lines = sorted(set(lines))

    with open(file_path, 'w') as file:
        file.writelines(sorted_unique_lines)

def find_duplicates_in_file(file_path):
    hash_counts = defaultdict(list)
    with open(file_path, 'r') as file:
        for line in file:
            hash_value, doc_id = line.strip().split(',')
            hash_counts[hash_value].append(doc_id)
    duplicates = {hash_value: doc_ids for hash_value, doc_ids in hash_counts.items() if len(doc_ids) > 1}
    return duplicates

def fetch_documents_by_hash(es, index_name, hash_value):
    attempt = 0
    while attempt < RETRY_LIMIT:
        try:
            response = es.search(index=index_name, body={
                'query': {
                    'match': {'hash': hash_value}
                }
            }, size=SCROLL_SIZE)
            return response['hits']['hits']
        except Exception as e:
            attempt += 1
            log_message(f"Error fetching documents by hash (Attempt {attempt}): {e}", 'error.log', level='error')
            if attempt < RETRY_LIMIT:
                time.sleep(RETRY_DELAY)
            else:
                return []

def delete_entry(es, index_name, doc_id):
    try:
        es.delete(index=index_name, id=doc_id)
        log_message(f"Request: Delete document ID {doc_id} from index {index_name}", level='info')
        return True
    except Exception as e:
        log_message(f"Error deleting entry: {e}", 'error.log', level='error')
        return False

def main():
    parser = argparse.ArgumentParser(description='Check for duplicate hashes in Elasticsearch')
    parser.add_argument('--delete-without-asking', action='store_true', help='Automatically delete duplicates without asking for confirmation')
    args = parser.parse_args()

    if args.delete_without_asking:
        print("WARNING: You have enabled automatic deletion of duplicates. All duplicates will be deleted without asking for confirmation. Proceed with caution.")

    if not os.path.exists(INDEXES_FILE):
        print(f"ERROR: The file '{INDEXES_FILE}' does not exist.")
        log_message(f"ERROR: The file '{INDEXES_FILE}' does not exist.", 'error.log', level='error')
        return

    with open(INDEXES_FILE, 'r') as file:
        index_names = [line.strip() for line in file.readlines()]

    try:
        es = Elasticsearch(
            hosts=ELASTICSEARCH_HOSTS,
            basic_auth=ELASTICSEARCH_AUTH,
            verify_certs=False,  # Disable SSL verification for testing
            request_timeout=60,  # Increase the timeout to handle long queries
            max_retries=10,  # Increase the number of retries
            retry_on_timeout=True
        )

        log_message("=============Duplicate Check Script started=============")

        for index_name in index_names:
            log_message(f"Index: {index_name}")

            temp_file_path = fetch_all_hashes(es, index_name)
            if not temp_file_path:
                log_message(f"Failed to fetch hashes for index {index_name}.")
                continue

            sort_and_deduplicate_file(temp_file_path)

            duplicates = find_duplicates_in_file(temp_file_path)

            if not duplicates:
                log_message(f"No duplicates found in index {index_name}.")
                continue

            total_duplicates = sum(len(doc_ids) for doc_ids in duplicates.values())
            
            with tqdm(total=total_duplicates, unit='entry', desc=f"Processing duplicates in {index_name}") as progress_bar:
                for hash_value, doc_ids in duplicates.items():
                    log_message(f"Found duplicate hash: {hash_value} (Count: {len(doc_ids)})")

                    documents = fetch_documents_by_hash(es, index_name, hash_value)
                    if not documents:
                        continue

                    for doc in documents:
                        doc_id = doc['_id']
                        source = doc['_source']
                        print(f"Duplicate found - ID: {doc_id}, Data: {source}")

                        if args.delete_without_asking:
                            if delete_entry(es, index_name, doc_id):
                                log_message(f"Deleted entry ID: {doc_id}", 'deletions.log', level='info')
                            else:
                                log_message(f"Failed to delete entry ID: {doc_id}", 'error.log', level='error')
                        else:
                            while True:
                                user_input = input(f"Do you want to delete this entry? (ID: {doc_id}) [y/n]: ").strip().lower()
                                if user_input in ['y', 'n']:
                                    break
                                print("Invalid input. Please enter 'y' or 'n'.")

                            if user_input == 'y':
                                if delete_entry(es, index_name, doc_id):
                                    log_message(f"Deleted entry ID: {doc_id}", 'deletions.log', level='info')
                                else:
                                    log_message(f"Failed to delete entry ID: {doc_id}", 'error.log', level='error')

                        progress_bar.update(1)

        log_message("=============Duplicate Check Script finished=============\n")

    except Exception as e:
        log_message(f"Script error: {e}", 'error.log', level='error')

if __name__ == '__main__':
    main()