#!/usr/bin/env python3
import json
import os
import sys
import subprocess
import tempfile
import difflib
from typing import Dict, Any
from pathlib import Path
import concurrent.futures
from collections import OrderedDict

temp_unrec = tempfile.mktemp(prefix="unrecognized_")
err_file_rc = tempfile.mktemp(prefix="dencoder_err_")

fast_shouldnt_skip = []
backward_compat: Dict[str, Any] = {}
incompat_paths: Dict[str, Any] = {}

def sort_values(obj):
    if isinstance(obj, dict):
        return OrderedDict((k, sort_values(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(obj, key=sort_list_values)
    return obj

def sort_list_values(obj):
    if isinstance(obj, dict):
        return sorted(obj.items())
    if isinstance(obj, list):
        return sorted(obj, key=sort_list_values)
    return obj


def process_type(file_path, type):
    print(f"dencoder test for {file_path}")
    cmd1 = [CEPH_DENCODER, "type", type, "import", file_path, "decode", "dump_json"]
    cmd2 = [CEPH_DENCODER, "type", type, "import", file_path, "decode", "encode", "decode", "dump_json"]

    output1 = ""
    output2 = ""
    try:
        result1 = subprocess.run(cmd1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output1 = result1.stdout.decode('unicode_escape')
        result2 = subprocess.run(cmd2, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output2 = result2.stdout.decode('unicode_escape')

        if result1.returncode != 0 or result2.returncode != 0:
            debug_print(f"**** reencode of {file_path} resulted in wrong return code ****")
            print(f"Error encountered in subprocess. Command: {cmd1}")
            print(f"Return code: {result1.returncode} Command:{result1.args} Output: {result1.stdout.decode('unicode_escape')}")
            print(f"Error encountered in subprocess. Command: {cmd2}")
            print(f"Return code: {result2.returncode} Command:{result2.args} Output: {result2.stdout.decode('unicode_escape')}")
            
            with open(err_file_rc, "a") as f:
                f.write(f"{type} -- {file_path}")
                f.write("\n")
            return 1

        if output1 != output2:
            cmd_determ = [CEPH_DENCODER, "type", type, "is_deterministic"]
            determ_res = subprocess.run(cmd_determ, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
             # Check if the command failed
            if determ_res.returncode != 0:
                error_message = determ_res.stderr.decode().strip()
                debug_print(f"Error running command: {error_message}")
                return 1

            json_output1 = json.loads(output1)
            sorted_json_output1 = json.dumps(sort_values(json_output1), indent=4)
            json_output2 = json.loads(output2)
            sorted_json_output2 = json.dumps(sort_values(json_output2), indent=4)
            if sorted_json_output1 == sorted_json_output2:
                debug_print(f"non-deterministic type {type} passed the test")
                return 0
            
            debug_print(f"**** reencode of {file_path} resulted in a different dump ****")
            diff_output = "\n".join(difflib.ndiff(output1.splitlines(), output2.splitlines()))
            diff_file   = tempfile.mktemp(prefix=f"diff_{type}_{file_path.name}_")
            with open(diff_file, "w") as f:
                f.write(diff_output)
            print(f"Different output for {file_path}:\n{diff_output}")
            return 1  # File failed the test

    except subprocess.CalledProcessError as e:
        print(f"Error encountered in subprocess. Command: {cmd1}")
        print(f"Return code: {e.returncode} Command:{e.cmd} Output: {e.output}")
        return 1

    except UnicodeDecodeError as e:
        print(f"Unicode Error encountered in subprocess. Command: {cmd1}")
        print(f"Return code: {e.returncode} Command:{e.cmd} Output: {e.output}")
        return 1

    return 0  # File passed the test

def test_object_wrapper(type, vdir, arversion, current_ver):
    global incompat_paths
    _numtests = 0
    _failed = 0
    unrecognized = ""

    if subprocess.call([CEPH_DENCODER, "type", type], stderr=subprocess.DEVNULL) == 0:

        if should_skip_object(type, arversion, current_ver) and (type not in incompat_paths or len(incompat_paths[type]) == 0):
            debug_print(f"skipping object of type {type} due to backward incompatibility")
            return (_numtests, _failed, unrecognized)

        debug_print(f"        {vdir}/objects/{type}")
        files = list(vdir.joinpath("objects", type).glob('*'))
        files_without_incompat = []

        # Check symbolic links
        if type in incompat_paths:
            incompatible_files = set(incompat_paths[type])
            files_without_incompat = [f for f in files if f.name not in incompatible_files]
        else:
            files_without_incompat = files

        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = [executor.submit(process_type, f, type) for f in files_without_incompat]

            for result in concurrent.futures.as_completed(results):
                _numtests += 1
                _failed += result.result()
    else:
        unrecognized = type
        debug_print("skipping unrecognized type {} return {}".format(type, (_numtests, _failed, unrecognized)))
        
    return (_numtests, _failed, unrecognized)

def should_skip_object(type, arversion, current_ver):
    """
    Check if an object of a specific type should be skipped based on backward compatibility.

    Description:
    This function determines whether an object of a given type should be skipped based on the
    provided versions and backward compatibility information. It checks the global variable
    'backward_compat' to make this decision.

    Input:
    - type: str
        The type of the object to be checked for skipping.

    - arversion: str
        The version from which the object is attempted to be accessed (archive version).

    - current_ver: str
        The version of the object being processed (current version).

    Output:
    - bool:
        True if the object should be skipped, False otherwise.

    Note: The function relies on two global variables, 'backward_compat' and 'fast_shouldnt_skip',
    which should be defined and updated appropriately in the calling code.
    """
    global backward_compat
    global fast_shouldnt_skip

    if type in fast_shouldnt_skip:
        debug_print(f"fast Type {type} does not exist in the backward compatibility structure.")
        return False

    if all(type not in v for v in backward_compat.values()):
        fast_shouldnt_skip.append(type)
        return False

    versions = [key for key, value in backward_compat.items() if type in value and key >= arversion and key != current_ver]
    if len(versions) == 0:
        return False

    return True

def check_backward_compat():
    """
    Check backward compatibility and collect incompatible paths for different versions and types.

    Description:
    This function scans the 'archive' directory and identifies backward incompatible paths
    for each version and type in the archive. It creates dictionaries '_backward_compat' and
    '_incompat_paths_all' to store the results.

    Input:
    - None (No explicit input required)

    Output:
    - _backward_compat: dict
        A nested dictionary containing backward incompatible paths for each version and type.
        The structure is as follows:
        {
            "version_name1": {
                "type_name1": ["incompat_path1", "incompat_path2", ...],
                "type_name2": ["incompat_path3", "incompat_path4", ...],
                ...
            },
            "version_name2": {
                ...
            },
            ...
        }
        
    - _incompat_paths_all: dict
        A dictionary containing all backward incompatible paths for each type across all versions.
        The structure is as follows:
        {
            "type_name1": ["incompat_path1", "incompat_path2", ...],
            "type_name2": ["incompat_path3", "incompat_path4", ...],
            ...
        }

    Note: The function uses the global variable 'DIR', which should be defined in the calling code.

    """
    _backward_compat = {}
    _incompat_paths_all = {}
    archive_dir = Path(os.path.join(DIR, 'archive'))
    
    if archive_dir.exists() and archive_dir.is_dir():
        for version in archive_dir.iterdir():
            if version.is_dir():
                version_name = version.name
                _backward_compat[version_name] = {}
                type_dir = archive_dir / version_name / "forward_incompat"
                if type_dir.exists() and type_dir.is_dir():
                    for type_entry in type_dir.iterdir():
                        if type_entry.is_dir():
                            type_name = type_entry.name
                            type_path = type_dir / type_name
                            if type_path.exists() and type_path.is_dir():
                                _incompat_paths = [incompat_entry.name for incompat_entry in type_path.iterdir() if incompat_entry.is_dir() or 
                                                                                                                incompat_entry.is_file() or 
                                                                                                                incompat_entry.is_symlink()]
                                _backward_compat[version_name][type_name] = _incompat_paths
                                _incompat_paths_all[type_name] = _incompat_paths
                                _incompat_paths = []
                        else:
                            _backward_compat[version_name][type_entry.name] = []
    debug_print(f"backward_compat: {_backward_compat}")
    debug_print(f"incompat_paths: {_incompat_paths_all}")

    return _backward_compat, _incompat_paths_all

def process_batch(batch):
    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(
                test_object_wrapper, batch_type, vdir, arversion, current_ver
            )
            for batch_type, vdir, arversion, current_ver in batch
        ]

        for future in concurrent.futures.as_completed(futures):
            result_tuple = future.result()
            results.append(result_tuple)

    return results

# Create a generator that processes batches asynchronously
def async_process_batches(task_batches):
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_batch, batch) for batch in task_batches]
        for future in concurrent.futures.as_completed(futures):
            yield future.result()

def debug_print(msg):
    if debug:
        print("DEBUG: {}".format(msg))


def main():
    global backward_compat
    global incompat_paths

    failed = 0
    numtests = 0
    task_batches = []
    current_batch = []
    batch_size = 100
    
    backward_compat, incompat_paths = check_backward_compat()
    debug_print(f'found {len(backward_compat)} backward incompatibilities')

    for arversion_entry in sorted(DIR.joinpath("archive").iterdir(), key=lambda entry: entry.name):
        arversion = arversion_entry.name
        vdir = Path(DIR.joinpath("archive", arversion))

        if not arversion_entry.is_dir() or not vdir.joinpath("objects").is_dir():
            debug_print("skipping non-directory {}".format(arversion))
            continue

        for type_entry in vdir.joinpath("objects").iterdir():
            type = type_entry.name
            current_batch.append((type, vdir, arversion, current_ver))
            if len(current_batch) >= batch_size:
                task_batches.append(current_batch)
                current_batch = []

    if len(current_batch) > 0:
        task_batches.append(current_batch)
    
    full_unrecognized = []
    for results in async_process_batches(task_batches):
        for result in results:
            _numtests, _failed, unrecognized = result
            debug_print("numtests: {}, failed: {}".format(_numtests, _failed))
            numtests += _numtests
            failed += _failed
            if unrecognized.strip() != '':
                full_unrecognized.append(unrecognized)
    
    if full_unrecognized is not None and len(full_unrecognized) > 0:
        with open(temp_unrec, "a") as file_unrec:
            file_unrec.writelines(line + "\n" for line in full_unrecognized)

    if failed > 0:
        print("FAILED {}/{} tests.".format(failed, numtests))
        return 1

    if numtests == 0:
        print("FAILED: no tests found to run!")

    print("Passed {} tests.".format(numtests))
    return 0

if __name__ == "__main__":
    if len(sys.argv) < 1:
        print(f"usage: {sys.argv[0]} <corpus-dir>")
        sys.exit(1)

    DIR = Path(sys.argv[1])
    CEPH_DENCODER = "ceph-dencoder"
    subprocess.run([CEPH_DENCODER, 'version'], check=True)
    current_ver = subprocess.check_output([CEPH_DENCODER, "version"]).decode().strip()
    debug = False
    ret = main()
    sys.exit(ret)
