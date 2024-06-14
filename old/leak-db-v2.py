import argparse
import os
import hashlib
from tqdm import tqdm
from elasticsearch import Elasticsearch, exceptions as elasticsearch_exceptions
from datetime import datetime

ELASTICSEARCH_HOSTS = ['https://localhost:9200']
ELASTICSEARCH_AUTH = ('elastic', 'password')
LOGS_DIR = 'logs'

def create_index(es, index_name, properties):
    es.indices.create(index=index_name, ignore=400, body={
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

def insert_new_entry(es, index_name, timestamp, hash_value, user=None, password=None, url=None):
    try:
        es.index(index=index_name, body={
            'timestamp': timestamp,
            'hash': hash_value,
            'user': user,
            'pass': password,
            'url': url
        })
        return True
    except Exception as e:
        log_message(f"Error inserting new entry: {e}", 'error.log', level='error')
        return False

def main():
    try:
        parser = argparse.ArgumentParser(description='Leak Database')
        parser.add_argument('--combolist', action='store_true', help='Process combolist file')
        parser.add_argument('--infostealer', action='store_true', help='Process infostealer file')
        parser.add_argument('file_path', type=str, help='Path to the input file')
        args = parser.parse_args()

        if args.combolist:
            index_name = 'combolists-leaks'
            properties = {
                'timestamp': {'type': 'date', 'format': 'strict_date_optional_time||epoch_second'},
                'hash': {'type': 'keyword'},
                'user': {'type': 'text'},
                'pass': {'type': 'text'}
            }
            delimiter = ':'
        elif args.infostealer:
            index_name = 'infostealer-leaks'
            properties = {
                'timestamp': {'type': 'date', 'format': 'strict_date_optional_time||epoch_second'},
                'hash': {'type': 'keyword'},
                'url': {'type': 'text'},
                'user': {'type': 'text'},
                'pass': {'type': 'text'}
            }
            delimiter = ','
        else:
            print("Error: You must specify either --combolist or --infostealer.")
            return

        log_message("=============Script started=============")
        log_message(f"Index: {index_name}")

        if not verify_file(args.file_path):
            log_message(f"File verification failed for '{args.file_path}'", 'error.log', level='error')
            return

        es = Elasticsearch(
            hosts=ELASTICSEARCH_HOSTS,
            basic_auth=ELASTICSEARCH_AUTH,
            verify_certs=False
        )

        create_index(es, index_name, properties)

        with open(args.file_path, 'r') as input_file:
            total_lines = sum(1 for _ in input_file)
            input_file.seek(0)

            with tqdm(total=total_lines, unit='line') as progress_bar:
                for line in input_file:
                    fields = line.strip().split(delimiter)

                    try:
                        timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z')

                        if args.combolist and len(fields) == 2:
                            user, password = fields
                            hash_value = calculate_hash(user + password)

                            if entry_exists(es, index_name, hash_value):
                                log_message(f"Entry already exists: {user}:{password}", level='info')
                            else:
                                if insert_new_entry(es, index_name, timestamp, hash_value, user=user, password=password):
                                    log_message(f"Inserted new entry: {user}:{password}", level='info')

                        elif args.infostealer and len(fields) == 3:
                            url, user, password = fields
                            hash_value = calculate_hash(url + user + password)

                            if entry_exists(es, index_name, hash_value):
                                log_message(f"Entry already exists: {url}:{user}:{password}", level='info')
                            else:
                                if insert_new_entry(es, index_name, timestamp, hash_value, user=user, password=password, url=url):
                                    log_message(f"Inserted new entry: {url}:{user}:{password}", level='info')

                        else:
                            log_message(f"Invalid input for {'--combolist' if args.combolist else '--infostealer'}: {line}", 'error.log', level='error')

                    except elasticsearch_exceptions.RequestError as e:
                        log_message(f"Parsing exception for entry: {line}\nError: {e}", 'error.log', level='error')

                    except Exception as e:
                        log_message(f"Error processing entry: {line}\nError: {e}", 'error.log', level='error')

                    progress_bar.update(1)

        log_message("=============Script finished=============\n")

    except KeyboardInterrupt:
        log_message("Script interrupted by user.", level='info')

if __name__ == '__main__':
    main()