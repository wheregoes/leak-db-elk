import warnings
from elasticsearch import Elasticsearch, exceptions as elasticsearch_exceptions
import urllib3
import os
from datetime import datetime
import argparse

# Suppress the InsecureRequestWarning from urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Suppress the specific Elasticsearch SecurityWarning
warnings.filterwarnings('ignore', category=UserWarning, module='elasticsearch')

ELASTICSEARCH_HOSTS = ['https://localhost:9200']  # Change to 'http://' if using HTTP
ELASTICSEARCH_AUTH = ('elastic', 'changeme')
LOGS_DIR = 'logs'
INDEXES_FILE = 'indexes-to-delete.txt'

def log_message(message, log_file_path='script.log', level='info'):
    log_levels = {'info': 'INFO', 'warning': 'WARNING', 'error': 'ERROR'}
    log_level = log_levels.get(level.lower(), 'INFO')
    timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z')
    
    log_dir = os.path.join(LOGS_DIR)
    os.makedirs(log_dir, exist_ok=True)

    with open(os.path.join(log_dir, log_file_path), 'a') as log_file:
        log_file.write(f"{timestamp} - {log_level} - {message}\n")
        log_file.flush()

def delete_index(es, index_name):
    try:
        es.indices.delete(index=index_name)
        log_message(f"Request: Delete index {index_name}", level='info')
        return True
    except Exception as e:
        log_message(f"Error deleting index {index_name}: {e}", 'error.log', level='error')
        return False

def main():
    parser = argparse.ArgumentParser(description='Delete indices in Elasticsearch')
    parser.add_argument('--delete-without-asking', action='store_true', help='Automatically delete indices without asking for confirmation')
    args = parser.parse_args()

    if args.delete_without_asking:
        print("WARNING: You have enabled automatic deletion of indices. All indices listed in the file will be deleted without asking for confirmation. Proceed with caution.")

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

        log_message("=============Index Deletion Script started=============")

        for index_name in index_names:
            log_message(f"Index: {index_name}")

            if args.delete_without_asking:
                if delete_index(es, index_name):
                    log_message(f"Deleted index: {index_name}", 'deletions.log', level='info')
                else:
                    log_message(f"Failed to delete index: {index_name}", 'error.log', level='error')
            else:
                while True:
                    user_input = input(f"Do you want to delete this index? (Index: {index_name}) [y/n]: ").strip().lower()
                    if user_input in ['y', 'n']:
                        break
                    print("Invalid input. Please enter 'y' or 'n'.")

                if user_input == 'y':
                    if delete_index(es, index_name):
                        log_message(f"Deleted index: {index_name}", 'deletions.log', level='info')
                    else:
                        log_message(f"Failed to delete index: {index_name}", 'error.log', level='error')

        log_message("=============Index Deletion Script finished=============\n")

    except Exception as e:
        log_message(f"Script error: {e}", 'error.log', level='error')

if __name__ == '__main__':
    main()