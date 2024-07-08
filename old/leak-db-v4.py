import warnings
from elasticsearch import Elasticsearch, exceptions as elasticsearch_exceptions
import urllib3
import argparse
import os
import hashlib
from tqdm import tqdm
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Suppress the InsecureRequestWarning from urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Suppress the specific Elasticsearch SecurityWarning
warnings.filterwarnings('ignore', category=UserWarning, module='elasticsearch')

ELASTICSEARCH_HOSTS = ['https://localhost:9200']
ELASTICSEARCH_AUTH = ('elastic', 'changeme')
LOGS_DIR = 'logs'

MAX_THREADS = 50000  # Adjust the number of threads based on your system capabilities
CHUNK_SIZE = 100  # Number of lines to process in each chunk

def create_index(es, index_name, properties):
    es.options(ignore_status=400).indices.create(index=index_name, body={
        'mappings': {
            'properties': properties
        }
    })

def verify_file(file_path):
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found.")
        return False
    return True

def calculate_hash(data):
    return hashlib.sha256(data.encode()).hexdigest()

def log_message(message, log_file_path='script.log', level='info'):
    log_levels = {'info': 'INFO', 'warning': 'WARNING', 'error': 'ERROR'}
    log_level = log_levels.get(level.lower(), 'INFO')
    timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z')
    
    log_dir = os.path.join(LOGS_DIR)
    os.makedirs(log_dir, exist_ok=True)

    with open(os.path.join(log_dir, log_file_path), 'a') as log_file:
        log_file.write(f"{timestamp} - {log_level} - {message}\n")
        log_file.flush()

def entry_exists(es, index_name, hash_value):
    try:
        response = es.search(index=index_name, body={
            'query': {
                'match': {'hash': hash_value}
            }
        })
        return response['hits']['total']['value'] > 0
    except Exception as e:
        log_message(f"Error checking entry existence: {e}", 'error.log', level='error')
        return False

def insert_new_entry(es, index_name, timestamp, hash_value, user=None, password=None, url=None, tag=None):
    try:
        es.index(index=index_name, body={
            'timestamp': timestamp,
            'hash': hash_value,
            'user': user,
            'pass': password,
            'url': url,
            'tag': tag
        })
        return True
    except Exception as e:
        log_message(f"Error inserting new entry: {e}", 'error.log', level='error')
        return False

def process_line(es, line, index_name, delimiter, tag, combolist):
    fields = line.strip().split(delimiter)
    timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z')
    
    try:
        if combolist and len(fields) == 2:
            user, password = fields
            hash_value = calculate_hash(user + password)
            if entry_exists(es, index_name, hash_value):
                log_message(f"Entry already exists: {user}:{password}", level='info')
            else:
                if insert_new_entry(es, index_name, timestamp, hash_value, user=user, password=password, tag=tag):
                    log_message(f"Inserted new entry: {user}:{password}", level='info')
        elif not combolist and len(fields) == 3:
            url, user, password = fields
            hash_value = calculate_hash(url + user + password)
            if entry_exists(es, index_name, hash_value):
                log_message(f"Entry already exists: {url}:{user}:{password}", level='info')
            else:
                if insert_new_entry(es, index_name, timestamp, hash_value, user=user, password=password, url=url, tag=tag):
                    log_message(f"Inserted new entry: {url}:{user}:{password}", level='info')
        else:
            log_message(f"Invalid input: {line}", 'error.log', level='error')
    except elasticsearch_exceptions.RequestError as e:
        log_message(f"Parsing exception for entry: {line}\nError: {e}", 'error.log', level='error')
    except Exception as e:
        log_message(f"Error processing entry: {line}\nError: {e}", 'error.log', level='error')

def main():
    try:
        parser = argparse.ArgumentParser(description='Leak Database')
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('--combolist', action='store_true', help='Process combolist file')
        group.add_argument('--infostealer', action='store_true', help='Process infostealer file')
        parser.add_argument('file_path', type=str, help='Path to the input file')
        parser.add_argument('--TAG', type=str, required=True, help='Tag to identify the source of the imports')
        args = parser.parse_args()

        if args.combolist:
            index_name = 'combolists-leaks'
            properties = {
                'timestamp': {'type': 'date', 'format': 'strict_date_optional_time||epoch_second'},
                'hash': {'type': 'keyword'},
                'user': {'type': 'text'},
                'pass': {'type': 'text'},
                'tag': {'type': 'text'}
            }
            delimiter = ':'
        elif args.infostealer:
            index_name = 'infostealer-leaks'
            properties = {
                'timestamp': {'type': 'date', 'format': 'strict_date_optional_time||epoch_second'},
                'hash': {'type': 'keyword'},
                'url': {'type': 'text'},
                'user': {'type': 'text'},
                'pass': {'type': 'text'},
                'tag': {'type': 'text'}
            }
            delimiter = ','

        log_message("=============Script started=============")
        log_message(f"Index: {index_name}")
        log_message(f"Tag: {args.TAG}")

        if not verify_file(args.file_path):
            log_message(f"File verification failed for '{args.file_path}'", 'error.log', level='error')
            return

        es = Elasticsearch(
            hosts=ELASTICSEARCH_HOSTS,
            basic_auth=ELASTICSEARCH_AUTH,
            verify_certs=False
        )

        create_index(es, index_name, properties)

        with open(args.file_path, 'r', errors='replace') as input_file:
            total_lines = sum(1 for _ in input_file)
            input_file.seek(0)

            with tqdm(total=total_lines, unit='line') as progress_bar:
                while True:
                    lines = input_file.readlines(CHUNK_SIZE)
                    if not lines:
                        break
                    
                    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                        futures = [executor.submit(process_line, es, line, index_name, delimiter, args.TAG, args.combolist) for line in lines]
                        
                        for future in as_completed(futures):
                            future.result()  # Ensure any exceptions are raised

                    progress_bar.update(len(lines))

        log_message("=============Script finished=============\n")

    except UnicodeDecodeError as e:
        log_message(f"Unicode decode error: {e}", 'error.log', level='error')
        return
    except KeyboardInterrupt:
        log_message("Script interrupted by user.", level='info')

if __name__ == '__main__':
    main()