import os
import numpy as np

def generate_int_pairs(num_pairs):
    a = np.random.randint(0, 1000001, size=num_pairs, dtype=np.int32)
    b = np.random.randint(0, 1000001, size=num_pairs, dtype=np.int32)
    return np.column_stack((a, b))

def generate_float_pairs(num_pairs):
    a = np.random.uniform(0.0, 1000000.0, size=num_pairs).astype(np.float32)
    b = np.random.uniform(0.0, 1000000.0, size=num_pairs).astype(np.float32)
    return np.column_stack((a, b))

def generate_double_pairs(num_pairs):
    a = np.random.uniform(0.0, 1000000.0, size=num_pairs)
    b = np.random.uniform(0.0, 1000000.0, size=num_pairs)
    return np.column_stack((a, b))

def write_pairs(file_path, pairs):
    with open(file_path, 'wb') as f:
        pairs.tofile(f)  # Use numpy's tofile for efficient binary writing

def create_binary_relation(args):
    file_path, num_pairs, data_type = args
    if data_type == 'int':
        pairs = generate_int_pairs(num_pairs)
    elif data_type == 'float':
        pairs = generate_float_pairs(num_pairs)
    elif data_type == 'double':
        pairs = generate_double_pairs(num_pairs)

    write_pairs(file_path, pairs)

def init_data_folder():
    if not os.path.exists('./data'):
        os.makedirs('./data')

    relations = {
        'relation_int_small.bin': ('int', 5_000),
        'relation_int.bin': ('int', 250_000_000),
        'relation_int_large.bin': ('int', 4*250_000_000),
        'relation_float.bin': ('float', 125_000_000),
        'relation_double.bin': ('double', 62_500_000),
    }

    args = [(os.path.join('./data', file_name), num_pairs, data_type)
            for file_name, (data_type, num_pairs) in relations.items()]

    for arg in args:
        print(f"Creating {arg[0]}")
        create_binary_relation(arg)
        print(f"Created {arg[0]}")

if __name__ == "__main__":
    init_data_folder()
