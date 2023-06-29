import os
import sys
import shutil
import subprocess
import tempfile
from multiprocessing import Pool

def export_encoded_object(args):
    type_name, test_num, temp_file = args
    subprocess.run(["bin/ceph-dencoder",
                    "type",
                    type_name,
                    "select_test",
                    str(test_num),
                    "encode",
                    "export",
                    temp_file
                    ], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def copy_objects(archive):
    if not os.path.isdir(archive):
        print(f"usage: {sys.argv[0]} <archive>")
        return

    ver = subprocess.run(["bin/ceph-dencoder",
                          "version"
                          ], stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.strip().decode()
    print(f"version {ver}")

    dest_dir = os.path.join(archive, ver, "objects")
    os.makedirs(dest_dir, exist_ok=True)

    temp_dir = tempfile.mkdtemp()

    type_list_output = subprocess.run(["bin/ceph-dencoder", "list_types"], stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.strip().decode()
    type_list = type_list_output.split("\n")

    pool = Pool()  # Use a process pool for parallel execution

    for type_name in type_list:
        type_dir = os.path.join(dest_dir, type_name)
        os.makedirs(type_dir, exist_ok=True)

        num_output = subprocess.run(["bin/ceph-dencoder", "type", type_name, "count_tests"], stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.strip().decode()
        num = int(num_output)

        args_list = [(type_name, test_num, os.path.join(temp_dir, f"{type_name}_{test_num}")) for test_num in range(num)]

        pool.map(export_encoded_object, args_list)  # Parallel execution of encoding

        for args in args_list:
            _, _, temp_file = args
            md5_output = subprocess.run(["md5sum", temp_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.strip().decode()
            md5_hash = md5_output.split()[0]

            dest_file = os.path.join(type_dir, md5_hash)
            if not os.path.exists(dest_file):
                shutil.copy2(temp_file, dest_file)

    pool.close()
    pool.join()

    shutil.rmtree(temp_dir)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <archive>")
        sys.exit(1)

    archive_dir = sys.argv[1]

    copy_objects(archive_dir)
