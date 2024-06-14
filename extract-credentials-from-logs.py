import os
import re
import sys
import csv

def rename_folders(directory):
    for root, dirs, _ in os.walk(directory):
        for dir_name in dirs:
            new_name = re.sub(r'[ \(\)\[\]!]','_', dir_name)
            if new_name != dir_name:
                old_path = os.path.join(root, dir_name)
                new_path = os.path.join(root, new_name)
                try:
                    os.rename(old_path, new_path)
                    print(f"Renamed '{old_path}' to '{new_path}'")
                except OSError as e:
                    print(f"Error renaming '{old_path}' to '{new_path}': {e}")

def search_password_files(directory, output_file_path):
    with open(output_file_path, 'w', encoding='utf-8', errors='ignore') as output_file:
        for root, _, files in os.walk(directory):
            for file_name in files:
                if re.search(r'password', file_name, re.IGNORECASE):
                    file_path = os.path.join(root, file_name)
                    try:
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                            output_file.write(f"Contents of {file_path}:\n")
                            output_file.write(file.read())
                            output_file.write("\n" + "="*40 + "\n")
                    except (OSError, IOError) as e:
                        print(f"Error reading file {file_path}: {e}")

def parse_txt_to_csv(txt_file_path, csv_file_path):
    entries = set()
    host, user, password = "", "", ""

    try:
        with open(txt_file_path, 'r', encoding='utf-8', errors='ignore') as fp:
            for line in fp:
                if ':' in line:
                    key, value = line.split(":", 1)
                    value = value.strip()
                    key_lower = key.lower()
                    if key_lower.startswith(('host', 'url')):
                        host = value
                    elif key_lower.startswith(('user', 'log', 'username', 'login')):
                        user = value
                    elif key_lower.startswith(('pass', 'pwd', 'password')):
                        password = value
                if line == '\n' or line.startswith(('==')):
                    if host or user or password:
                        entries.add((host, user, password))
                    host, user, password = "", "", ""
    except (OSError, IOError) as e:
        print(f"Error reading file {txt_file_path}: {e}")

    sorted_entries = sorted(entries)

    try:
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file, escapechar='\\', quoting=csv.QUOTE_NONE)
            for entry in sorted_entries:
                csv_writer.writerow(entry)
    except (OSError, IOError) as e:
        print(f"Error writing to file {csv_file_path}: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 extract-credentials-from-logs.py <path/to/folder/extract-passwords/>")
        sys.exit(1)

    directory_to_search = sys.argv[1]

    if not os.path.isdir(directory_to_search):
        print(f"The provided path '{directory_to_search}' is not a valid directory.")
        sys.exit(1)

    output_dir = os.path.join(directory_to_search, 'extracted-passwords-from-logs-folder')
    txt_file_path = os.path.join(output_dir, 'extracted-passwords-from-logs-folder.txt')
    csv_file_path = os.path.join(output_dir, 'extracted-passwords-from-logs-folder.csv')

    rename_folders(directory_to_search)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    search_password_files(directory_to_search, txt_file_path)

    parse_txt_to_csv(txt_file_path, csv_file_path)

    try:
        os.remove(txt_file_path)
    except (OSError, IOError) as e:
        print(f"Error removing file {txt_file_path}: {e}")

    print(f"Results have been saved to {csv_file_path}")