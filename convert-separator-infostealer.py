import re
import sys

def replace_colon_except_protocol(data):
    lines = data.strip().split('\n')
    result = []
    for line in lines:
        parts = re.split(r'(://)', line)
        for i in range(len(parts)):
            if parts[i] == '://':
                continue
            parts[i] = parts[i].replace(':', ',')
        result.append(''.join(parts))
    return '\n'.join(result)

if len(sys.argv) != 2:
    print("Usage: python3 script.py <input_file>")
    sys.exit(1)

input_file = sys.argv[1]

with open(input_file, 'r') as file:
    data = file.read()

modified_data = replace_colon_except_protocol(data)

print(modified_data)