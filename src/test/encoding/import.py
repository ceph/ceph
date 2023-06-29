import os
import sys
import shutil
import hashlib
import multiprocessing
import getopt

def copy_file(file_info, debug=False):
    src_file, dest_file = file_info
    shutil.copy2(src_file, dest_file)

    if debug:
        print(f"Copied: {src_file} --> {dest_file}")

def copy_objects(src, ver, archive, debug=False):
    if not os.path.isdir(archive) or not os.path.isdir(src):
        print(f"usage: {sys.argv[0]} <srcdir> <version> <archive>")
        return

    dest_dir = os.path.join(archive, ver, "objects")
    os.makedirs(dest_dir, exist_ok=True)

    files_to_copy = []
    for root, _, files in os.walk(src):
        for file in files:
            file_path = os.path.join(root, file)
            obj_type, _ = file.rsplit("__", maxsplit=1)
            file_hash = hashlib.md5(open(file_path, 'rb').read()).hexdigest()

            obj_dir = os.path.join(dest_dir, obj_type)
            os.makedirs(obj_dir, exist_ok=True)

            dest_file = os.path.join(obj_dir, file_hash)
            if not os.path.exists(dest_file):
                files_to_copy.append((file_path, dest_file))

    # Determine the number of CPU cores to utilize
    num_processes = multiprocessing.cpu_count()

    if debug:
        print(f"Number of CPU cores: {num_processes}")

    # Copy files in parallel
    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.starmap(copy_file, [(file_info, debug) for file_info in files_to_copy])

def main(argv):
    src_dir = ''
    version = ''
    archive_dir = ''
    debug_flag = False

    try:
        opts, args = getopt.getopt(argv, "", ["archive-path=", "dump-path=", "version=", "debug"])
    except getopt.GetoptError:
        print(f"usage: {sys.argv[0]} --archive-path <archive> --dump-path <srcdir> --version <version> [--debug]")
        sys.exit(2)

    for opt, arg in opts:
        if opt == "--archive-path":
            archive_dir = arg
        elif opt == "--dump-path":
            src_dir = arg
        elif opt == "--version":
            version = arg
        elif opt == "--debug":
            debug_flag = True

    copy_objects(src_dir, version, archive_dir, debug=debug_flag)

if __name__ == "__main__":
    main(sys.argv[1:])
