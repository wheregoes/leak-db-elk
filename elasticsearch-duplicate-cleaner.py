import warnings
from elasticsearch import Elasticsearch, exceptions as elasticsearch_exceptions
import urllib3
import os
from datetime import datetime
import argparse
import time
from tqdm import tqdm

# Suppress the InsecureRequestWarning from urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Suppress the specific Elasticsearch SecurityWarning
warnings.filterwarnings('ignore', category=UserWarning, module='elasticsearch')

ELASTICSEARCH_HOSTS = ['https://localhost:9200']  # Change to 'http://' if using HTTP
ELASTICSEARCH_AUTH = ('elastic', 'changeme')
LOGS_DIR = 'logs'
RETRY_LIMIT = 3
RETRY_DELAY = 5  # seconds
INDEXES_FILE = 'indexes-to-check.txt'

def log_message(message, log_file_path='script.log', level='info'):
    log_levels = {'info': 'INFO', 'warning': 'WARNING', 'error': 'ERROR'}
    log_level = log_levels.get(level.lower(), 'INFO')
    timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z')
    
    log_dir = os.path.join(LOGS_DIR)
    os.makedirs(log_dir, exist_ok=True)

    with open(os.path.join(log_dir, log_file_path), 'a') as log_file:
        log_file.write(f"{timestamp} - {log_level} - {message}\n")
        log_file.flush()

def find_duplicates(es, index_name):
    attempt = 0
    while attempt < RETRY_LIMIT:
        try:
            response = es.search(index=index_name, body={
                'size': 0,
                'aggs': {
                    'duplicate_hashes': {
                        'terms': {
                            'field': 'hash',
                            'min_doc_count': 2,
                            'size': 10000  # Adjust this size according to your needs
                        }
                    }
                }
            })
            log_message(f"Request: Search duplicates in index {index_name}", level='info')
            return response['aggregations']['duplicate_hashes']['buckets']
        except Exception as e:
            attempt += 1
            log_message(f"Error finding duplicates (Attempt {attempt}): {e}", 'error.log', level='error')
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
            verify_certs=False,  # Disable SSL verification
            request_timeout=60,  # Increase the timeout to handle long queries
            max_retries=10,  # Increase the number of retries
            retry_on_timeout=True
        )

        log_message("=============Duplicate Check Script started=============")

        for index_name in index_names:
            log_message(f"Index: {index_name}")

            duplicates = find_duplicates(es, index_name)

            if not duplicates:
                log_message(f"No duplicates found in index {index_name}.")
                continue

            total_duplicates = sum(duplicate['doc_count'] for duplicate in duplicates)
            with tqdm(total=total_duplicates, unit='entry') as progress_bar:
                for duplicate in duplicates:
                    hash_value = duplicate['key']
                    count = duplicate['doc_count']

                    log_message(f"Found duplicate hash: {hash_value} (Count: {count})")

                    response = es.search(index=index_name, body={
                        'query': {
                            'match': {'hash': hash_value}
                        }
                    })
                    log_message(f"Request: Search for hash {hash_value} in index {index_name}", level='info')

                    for hit in response['hits']['hits']:
                        doc_id = hit['_id']
                        source = hit['_source']
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