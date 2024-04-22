# sql_to_archive.py
#########################################################################################
# Author  : Hong
# Created : 4/19/2024
# Modified: 4/19/2024
# Notes   :
#########################################################################################
import shutil
import tarfile
import os
import pyAesCrypt
import sys
import datetime
import pyminizip
from dotenv import load_dotenv


# .env file
load_dotenv()

def progress_callback(file_index, total_files=4):
    sys.stdout.write(f"\rCompressing... {file_index}/{total_files}")
    sys.stdout.flush()

def bytes_to_gib(bytes):
    gib = bytes / (1024 ** 3)  # 1 GiB = 1024^3 bytes
    return gib


def get_next_file_number(file_prefix, target_dir):
    max_file_number = 0
    found_files = False

    for filename in os.listdir(target_dir):
        if filename.startswith(file_prefix):
            found_files = True
            file_parts = filename.split('-')
            if len(file_parts) == 2 and file_parts[1].endswith('.tar.aes'):
                try:
                    file_number = int(file_parts[1].split('.')[0])
                    max_file_number = max(max_file_number, file_number)
                except ValueError:
                    pass  # Ignore if the file number is invalid.

    if found_files:
        return max_file_number + 1
    else:
        return 1


def get_log_time():
    return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f%z")



def log_message(level, message):
    log_time = get_log_time()
    prefix = {
        'INFO': 'INFO',
        'WARN': 'WARNING',
        'ERROR': 'ERROR'
    }.get(level, 'INFO')

    print(f"{log_time}\t{prefix}\t{message}")
    sys.stdout.flush()

def sql_to_archive():
    source_dir='/storage/nextcloud/mysql_backup'
    target_dir='/storage/nextcloud/mysql_backup/0_ARCHIVE'
    completed_dir='/storage/nextcloud/mysql_backup/0_COMPLETED'
    max_size=30.0
    min_size=16.5
    timestamp = datetime.datetime.now().strftime("%Y%m%d")
    target_hours_ago = datetime.datetime.now() - datetime.timedelta(hours=1) # time ago from now
    target_files = sorted([f for f in os.listdir(source_dir) if f.endswith('.sql')], reverse=False)
    selected_files = []
    total_size = 0 
    password = os.getenv("PASSWORD")


    log_message("INFO", f"Initiating Archive Process for {source_dir}")

    
    # Create directories if they don't exist
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        log_message("INFO", f"Initiating target dir {target_dir}")

    if not os.path.exists(completed_dir):
        os.makedirs(completed_dir)
        log_message("INFO", f"Initiating completed dir {completed_dir}")


    # Get .sql file list
    for file_name in target_files:
        # Exit loop if total size meets the minimum size condition
        if total_size >= min_size * (1024 ** 3):
            break

        file_path = os.path.join(source_dir, file_name)
        file_size = os.path.getsize(file_path)  # File size in bytes
        file_m_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path)) # File modification time
        is_target = file_m_time < target_hours_ago

        file = {
            'file_path': file_path,
            'file_name': file_name,
            'file_size': file_size,
            'file_m_time': file_m_time,
            'is_target': is_target
        }

        # Select files modified within the last 1 hours and whose size fits within 16.5 ~ GiB
        if is_target and (total_size + file_size) <= max_size * (1024 ** 3):
            selected_files.append(file)
            log_message("INFO", f"appending files: {file}")
            total_size += file_size


    # Print a message and exit if no files are selected
    if not selected_files:
        log_message("WARN", "No files selected")
        return


    # Print a message and exit if total size does not meet the minimum size condition
    if total_size < min_size * (1024 ** 3):
        log_message("WARN", f"Not enough files collected. total size is {total_size} ({bytes_to_gib(total_size)}GiB)")
        return

    log_message("INFO", f"total_size is {total_size} ({bytes_to_gib(total_size)}GiB)")

    # Create archive
    file_prefix = f"{timestamp}-"
    archive_file_number = get_next_file_number(file_prefix, target_dir)
    archive_dir_name = f"{file_prefix}{archive_file_number:04}"
    archive_file_name = f"{archive_dir_name}.tar"
    aes_archive_file_name = f"{archive_dir_name}.tar.aes"
    log_file_name = f"{archive_dir_name}.log"
    archive_file_path = os.path.join(target_dir, archive_file_name)
    aes_archive_file_path = os.path.join(target_dir, aes_archive_file_name)
    log_file_path = os.path.join(target_dir, log_file_name)
    completed_file_dir = os.path.join(completed_dir, archive_dir_name)
    selected_file_paths = [file['file_path'] for file in selected_files] 

    try:
        log_message('INFO', f"Generating archive {selected_file_paths}")
        
        with tarfile.open(archive_file_path, "w") as tar:
            for selected_file in selected_files:
                file_path = selected_file['file_path']
                tar.add(file_path, arcname=selected_file['file_name'])


        buffer_size = 64 * 1024
        pyAesCrypt.encryptFile(archive_file_path, aes_archive_file_path, password, buffer_size)

        os.remove(archive_file_path)
        log_message('INFO', f"archive completed. {archive_file_path}")
    except Exception as e:
        log_message('ERROR', f"Error compressing files: {e}")



    # Move selected files to the completed directory
    if not os.path.exists(completed_file_dir):
        os.makedirs(completed_file_dir)

    for selected_file in selected_files:
        file_path = selected_file['file_path']
        file_name = selected_file['file_name']
        target_file_path = os.path.join(completed_file_dir, file_name)

        if os.path.exists(target_file_path):
            os.remove(target_file_path)  # Remove existing file

        shutil.move(file_path, completed_file_dir)


    with open(log_file_path, 'w') as log:
        log.write(f"# {log_file_name}\n\n")

        log.write("\nSelected Files:\n")
        for selected_file in selected_files:
            log.write(f"File Name: {selected_file['file_name']}\n")
            log.write(f"File Size: {selected_file['file_size']} bytes\n")
            log.write(f"File Modification Time: {selected_file['file_m_time']}\n")
            log.write(f"Is Target: {selected_file['is_target']}\n")
            log.write("\n")


if __name__ == '__main__':
    sql_to_archive();
