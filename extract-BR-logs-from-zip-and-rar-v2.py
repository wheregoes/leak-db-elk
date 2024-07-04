import os
import sys
import zipfile
import rarfile
import shutil
from tqdm import tqdm
import subprocess

def create_destination_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def get_next_available_folder_name(folder_path):
    base_folder_path = folder_path
    counter = 1
    while os.path.exists(folder_path):
        folder_path = f"{base_folder_path}_{counter:02d}"
        counter += 1
    return folder_path

def count_relevant_folders_in_zip(zip_path):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            return sum(1 for member in zip_ref.namelist() if member.endswith('/') and member.split('/')[-2].startswith('BR'))
    except (zipfile.BadZipFile, NotImplementedError) as e:
        print(f"Skipping {zip_path} due to error: {e}")
        return 0

def count_relevant_folders_in_rar(rar_path):
    try:
        with rarfile.RarFile(rar_path, 'r') as rar_ref:
            return sum(1 for member in rar_ref.infolist() if member.isdir() and member.filename.split('/')[-2].startswith('BR'))
    except rarfile.NeedFirstVolume as e:
        print(f"Skipping {rar_path} due to error: {e}")
        return 0

def extract_with_7z(archive_path, dest_folder, passwords, timeout=120):
    for password in passwords:
        command = ['7z', 'x', f'-p{password}', '-o' + dest_folder, archive_path]
        try:
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
            if result.returncode == 0:
                return
        except subprocess.TimeoutExpired:
            print(f"7z extraction for {archive_path} timed out")
            return
        except Exception as e:
            print(f"7z extraction for {archive_path} failed due to: {e}")
            return
    print(f"Failed to extract {archive_path} with provided passwords")

def extract_from_zip(zip_path, dest_folder, passwords):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            relevant_folders = [member for member in zip_ref.namelist() if member.endswith('/') and member.split('/')[-2].startswith('BR')]
            with tqdm(total=len(relevant_folders), desc=f"Processing {os.path.basename(zip_path)}") as pbar:
                for member in relevant_folders:
                    folder_name = member.split('/')[-2]
                    dest_path = get_next_available_folder_name(os.path.join(dest_folder, folder_name))
                    create_destination_dir(dest_path)
                    for file in zip_ref.namelist():
                        if file.startswith(member) and not file.endswith('/'):
                            extracted = False
                            for password in passwords:
                                try:
                                    with zip_ref.open(file, pwd=password.encode()) as source, \
                                         open(os.path.join(dest_path, os.path.basename(file)), 'wb') as target:
                                        shutil.copyfileobj(source, target)
                                    extracted = True
                                    break
                                except (RuntimeError, zipfile.BadZipFile, zipfile.LargeZipFile, NotImplementedError):
                                    continue
                            if not extracted:
                                print(f"Failed to extract {file} from {zip_path}")
                    pbar.update(1)
    except (zipfile.BadZipFile, zipfile.LargeZipFile, NotImplementedError) as e:
        print(f"Using 7z for {zip_path} due to: {e}")
        extract_with_7z(zip_path, dest_folder, passwords)
    except Exception as e:
        print(f"Failed to process {zip_path} due to unexpected error: {e}")

def extract_from_rar(rar_path, dest_folder, passwords):
    try:
        with rarfile.RarFile(rar_path, 'r') as rar_ref:
            relevant_folders = [member for member in rar_ref.infolist() if member.isdir() and member.filename.split('/')[-2].startswith('BR')]
            with tqdm(total=len(relevant_folders), desc=f"Processing {os.path.basename(rar_path)}") as pbar:
                for member in relevant_folders:
                    folder_name = member.filename.split('/')[-2]
                    dest_path = get_next_available_folder_name(os.path.join(dest_folder, folder_name))
                    create_destination_dir(dest_path)
                    for file in rar_ref.infolist():
                        if file.filename.startswith(member.filename) and not file.isdir():
                            file_dest_path = os.path.join(dest_path, os.path.relpath(file.filename, member.filename))
                            os.makedirs(os.path.dirname(file_dest_path), exist_ok=True)
                            extracted = False
                            for password in passwords:
                                try:
                                    with rar_ref.open(file, pwd=password) as source, open(file_dest_path, 'wb') as target:
                                        shutil.copyfileobj(source, target)
                                    extracted = True
                                    break
                                except rarfile.BadRarFile:
                                    continue
                            if not extracted:
                                print(f"Failed to extract {file.filename} from {rar_path}")
                    pbar.update(1)
    except rarfile.NeedFirstVolume as e:
        print(f"Skipping {rar_path} due to error: {e}")
    except Exception as e:
        print(f"Failed to process {rar_path} due to unexpected error: {e}")

def is_first_volume(rar_path):
    base, ext = os.path.splitext(rar_path)
    if ext in ['.rar', '.part1.rar', '.part01.rar']:
        return True
    elif ext.startswith('.part') and ext.endswith('.rar'):
        try:
            part_num = int(ext[5:-4])
            return part_num == 1
        except ValueError:
            return False
    return False

def read_passwords(password_file):
    with open(password_file, 'r') as file:
        return [line.strip() for line in file]

def process_archives(archive_dir, dest_folder, password_file):
    create_destination_dir(dest_folder)
    passwords = read_passwords(password_file)
    archive_files = [os.path.join(root, file)
                     for root, _, files in os.walk(archive_dir)
                     for file in files if file.endswith('.zip') or (file.endswith('.rar') and is_first_volume(file))]

    total_relevant_folders = 0
    for file_path in archive_files:
        if file_path.endswith('.zip'):
            total_relevant_folders += count_relevant_folders_in_zip(file_path)
        elif file_path.endswith('.rar'):
            total_relevant_folders += count_relevant_folders_in_rar(file_path)

    with tqdm(total=total_relevant_folders, desc="Processing all archives") as pbar:
        for file_path in archive_files:
            try:
                if file_path.endswith('.zip'):
                    extract_from_zip(file_path, dest_folder, passwords)
                elif file_path.endswith('.rar'):
                    extract_from_rar(file_path, dest_folder, passwords)
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
            pbar.update(1)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 extract-BR-logs-from-zip-and-rar-v2.py <path/to/files> <path/to/passwords>")
        sys.exit(1)

    archive_dir = sys.argv[1]
    password_file = sys.argv[2]
    dest_folder = 'extracted-br-logs'

    process_archives(archive_dir, dest_folder, password_file)