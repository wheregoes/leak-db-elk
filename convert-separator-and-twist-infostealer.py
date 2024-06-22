import re
import sys

def replace_colon_except_protocol(data):
    lines = data.strip().split('\n')
    result = []
    protocols = ['https://', 'http://', 'android://', 'sftp://', 'ftp://', 'ssh://']
    
    for line in lines:
        protocol_found = False
        for protocol in protocols:
            if line.startswith(protocol):
                protocol_found = True
                break
        if not protocol_found:
            for protocol in protocols:
                if protocol in line:
                    parts = line.split(protocol)
                    parts[1] = protocol + parts[1]  # reattach protocol to the second part
                    reordered_line = parts[1] + ',' + parts[0].replace(':', ',')
                    result.append(reordered_line)
                    break
            else:
                parts = re.split(r'(://)', line)
                for i in range(len(parts)):
                    if parts[i] == '://':
                        continue
                    parts[i] = parts[i].replace(':', ',')
                result.append(''.join(parts))
        else:
            parts = re.split(r'(://)', line)
            for i in range(len(parts)):
                if parts[i] == '://':
                    continue
                parts[i] = parts[i].replace(':', ',')
            result.append(''.join(parts))
    
    return '\n'.join(result)

def process_file_in_chunks(input_file, chunk_size=1024):
    with open(input_file, 'rb') as file:
        while True:
            data = file.read(chunk_size)
            if not data:
                break
            # Ensure we don't break in the middle of a line
            if data[-1:] != b'\n':
                extra_data = file.readline()
                data += extra_data
            try:
                decoded_data = data.decode('utf-8')
                modified_data = replace_colon_except_protocol(decoded_data)
                print(modified_data, end='')
            except UnicodeDecodeError:
                continue

if len(sys.argv) < 2 or len(sys.argv) > 3:
    print("Usage: python3 script.py <input_file> [chunk_size]")
    sys.exit(1)

input_file = sys.argv[1]
chunk_size = int(sys.argv[2]) if len(sys.argv) == 3 else 1024

process_file_in_chunks(input_file, chunk_size)