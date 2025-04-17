#!/usr/bin/env python3

import yaml
import argparse
import sys
import json
import re

from git import Repo
import tempfile
from pathlib import Path

from collections import defaultdict

# Constants for Ceph repository and folder paths
CEPH_UPSTREAM_REMOTE_URL = "https://github.com/ceph/ceph.git"
CEPH_CONFIG_OPTIONS_FOLDER_PATH = "src/common/options"


def sparse_branch_checkout(repo_url: str, branch_name: str) -> tempfile.TemporaryDirectory[str]:
    """
    Clone a sparse branch and checkout the required folder.

    Args:
        repo_url (str): The repository URL to clone.
        branch_name (str): The branch name to checkout.
    """

    config_tmp_dir = tempfile.TemporaryDirectory()
    branch_name_str = "--branch=" + branch_name
    repo = Repo.clone_from(
        url=repo_url,
        to_path=config_tmp_dir.name,
        multi_options=[
            "--sparse",
            "--single-branch",
            branch_name_str,
            "--filter=blob:none",
            "--no-checkout",
            "--depth=1",
        ],
    )
    git_cmd = repo.git
    git_cmd.sparse_checkout("add", CEPH_CONFIG_OPTIONS_FOLDER_PATH)
    git_cmd.checkout()
    repo.close()

    return config_tmp_dir


def load_config_yaml_files(path: Path):
    """
    Load YAML configuration files from the given path.

    Args:
        path (Path): The directory path where the repository is stored.

    Returns:
        dict: A dictionary containing configuration options for each file.

    Raises:
        SystemExit: If any error occurs while reading or parsing YAML files.
    """
    config_paths = list(path.joinpath("src", "common", "options").glob("*.yaml.in"))

    if not config_paths:
        raise FileNotFoundError(f"No configuration YAML files found in directory: {path}")

    config_options = {}

    for path in config_paths:
        try:
            # Preprocess the file to enclose template value in double quotes
            # eg: @CEPH_INSTALL_FULL_PKGLIBDIR@/erasure-code -> "@CEPH_INSTALL_FULL_PKGLIBDIR@/erasure-code"
            # This pre-process is okay since these values are dependent on
            # the build system and as such cannot be found out until the
            # entire ceph is built - which is a cumbersome process

            file_content = path.read_text()
            # Enclose @key@somemoretext in double quotes "@key@somemoretext"
            file_content = re.sub(r"@.*@.*", r'"\g<0>"', file_content)
            config_options[path.name] = yaml.safe_load(file_content)
        except yaml.YAMLError as excep:
            print(excep)
            sys.exit(1)

    return config_options


def print_diff_posix_format(diff_result: dict):
    """
    Print the configuration differences in a POSIX diff-like format.

    Args:
        diff_result (dict): A dictionary containing added, deleted, and modified configurations.
    """

    # Handle added configurations
    for daemon, added_configs in diff_result.get("added", {}).items():
        for config in added_configs:
            print(f"+ added: {config}")

    # Handle deleted configurations
    for daemon, deleted_configs in diff_result.get("deleted", {}).items():
        for config in deleted_configs:
            print(f"- removed: {config}")

    # Handle modified configurations
    for daemon, modified_configs in diff_result.get("modified", {}).items():
        for config, changes in modified_configs.items():
            for key, change in changes.items():
                before = change.get("before", "")
                after = change.get("after", "")
                print(f"! changed: {config}: old: {before}")
                print(f"! changed: {config}: new: {after}")


def get_daemons_config_names(daemons, daemon_configs):
    """
    Get the names of all configuration options across all daemons.

    Args:
        daemons (set): A set of daemon names.
        daemon_configs (dict): A dictionary containing daemon configurations.

    Returns:
        dict: A dictionary mapping daemon names to their configuration option names.
    """
    daemons_config_names = defaultdict(list)
    for daemon in daemons:
        daemon_config_options = daemon_configs[daemon]["options"]
        daemon_config_names = set(
            map(lambda config_value: config_value["name"], daemon_config_options)
        )
        daemons_config_names[daemon] = list(daemon_config_names)
    return daemons_config_names


# Get the configuration options that has been modified, Returns a diction in the format:
def get_shared_config_daemon(shared_config_names, ref_daemon_configs, cmp_daemon_configs):
    """
    Get the configuration options that have been modified.

    Args:
        shared_config_names (set): A set of shared configuration option names.
        ref_daemon_configs (list): The reference daemon configurations.
        cmp_daemon_configs (list): The comparing daemon configurations.

    Returns:
        dict: A dictionary containing modified configuration options.

    Returns a dictionary in the format:

    "modified":{
            "<file-name-1>" :{
                "config-option-1": {
                    "key-1": {
                        "before": "<old-value>",
                        "after": "<new-value>"
                    },
                }
            }
    }
    """
    modified_config = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    for config_name in shared_config_names:
        # Get the entire config information for the configuration option
        ref_daemon_config = next(
            filter(
                lambda deamon_config: deamon_config["name"] == config_name,
                ref_daemon_configs,
            ),
            None,
        )
        cmp_daemon_config = next(
            filter(
                lambda deamon_config: deamon_config["name"] == config_name,
                cmp_daemon_configs,
            ),
            None,
        )

        # Get all the keys of a config option (eg: type, level, desc etc)
        ref_daemon_config_keys = set(ref_daemon_config.keys())
        cmp_daemon_config_keys = set(cmp_daemon_config.keys())

        # Get the new config option key that was added
        deleted_config_keys = ref_daemon_config_keys.difference(cmp_daemon_config_keys)

        # Get the config option key that was deleted
        new_config_keys = cmp_daemon_config_keys.difference(ref_daemon_config_keys)

        for config_key in new_config_keys:
            modified_config[config_name][config_key]["before"] = ""
            modified_config[config_name][config_key]["after"] = cmp_daemon_config[config_key]

        for config_key in deleted_config_keys:
            modified_config[config_name][config_key]["before"] = ref_daemon_config[config_key]
            modified_config[config_name][config_key]["after"] = ""

        shared_config_keys = ref_daemon_config_keys.intersection(cmp_daemon_config_keys)
        for config_key in shared_config_keys:
            if ref_daemon_config[config_key] != cmp_daemon_config[config_key]:
                modified_config[config_name][config_key]["before"] = ref_daemon_config[config_key]
                modified_config[config_name][config_key]["after"] = cmp_daemon_config[config_key]

    return modified_config


def diff_config(ref_repo_path: Path, cmp_repo_path: Path):
    """
    Perform the configuration diff between reference and comparing versions.

    Returns:
        dict: A dictionary containing added, deleted, and modified configurations.
    """
    new_config = defaultdict(list)
    deleted_config = defaultdict(list)
    modified_config = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))

    # Get the configurations options present for all daemons in the "reference" version
    ref_config_dict = load_config_yaml_files(ref_repo_path)
    ref_file_names = set(ref_config_dict.keys())

    # Get the configurations options present for all daemons in the "comparing" version
    config_dict = load_config_yaml_files(cmp_repo_path)
    cmp_file_names = set(config_dict.keys())

    # Case 1: A deamon is present in "reference" version but has been deleted
    # from "comparing" version
    # (A,B,C) ref - (A,B) cmp == C (new daemon)
    deleted_daemons = (ref_file_names).difference(cmp_file_names)
    deleted_config = get_daemons_config_names(deleted_daemons, ref_config_dict)

    # Case 2: A daemon is not present in "refrence" version but is
    # added/introduced in the "comparing" version
    # (A,B,C) cmp - (A,B) ref  = C (deleted daemon)
    new_daemons = cmp_file_names.difference(ref_file_names)
    new_config = get_daemons_config_names(new_daemons, config_dict)

    # Case 3: Compare the config options between the common daemons of
    # "reference" version and "comparing" version
    file_names = ref_file_names.intersection(cmp_file_names)
    for daemon in file_names:
        ref_daemon_configs = ref_config_dict[daemon]["options"]
        ref_daemon_config_names = set(
            map(lambda config_value: config_value["name"], ref_daemon_configs)
        )
        cmp_daemon_configs = config_dict[daemon]["options"]
        cmp_daemon_config_names = set(
            map(lambda config_value: config_value["name"], cmp_daemon_configs)
        )

        added = cmp_daemon_config_names.difference(ref_daemon_config_names)
        removed = ref_daemon_config_names.difference(cmp_daemon_config_names)

        new_config[daemon] = list(added)
        deleted_config[daemon] = list(removed)

        # get modified configs
        shared_config_names = ref_daemon_config_names.intersection(cmp_daemon_config_names)
        modified_config[daemon] = get_shared_config_daemon(
            shared_config_names, ref_daemon_configs, cmp_daemon_configs
        )

    # do not include daemons whose configurations have not changed
    new_config = {key: value for key, value in new_config.items() if len(value) != 0}
    deleted_config = {key: value for key, value in deleted_config.items() if len(value) != 0}
    modified_config = {key: value for key, value in modified_config.items() if len(value) != 0}

    final_result = defaultdict()
    final_result["added"] = new_config
    final_result["deleted"] = deleted_config
    final_result["modified"] = modified_config

    return final_result


def diff_branch(ref_repo: str, ref_branch: str, cmp_branch: str, is_posix_diff: bool):
    """
    Perform a diff between two branches in the same repository.

    Args:
        ref_repo (str): The reference repository URL.
        ref_branch (str): The reference branch name.
        cmp_branch (str): The branch to compare against.
    """
    ref_repo_tmp_dir = sparse_branch_checkout(ref_repo, ref_branch)
    cmp_repo_tmp_dir = sparse_branch_checkout(ref_repo, cmp_branch)

    final_result = diff_config(Path(ref_repo_tmp_dir.name), Path(cmp_repo_tmp_dir.name))

    if is_posix_diff:
        # Print the diff in POSIX format
        print_diff_posix_format(final_result)
    else:
        json.dump(final_result, sys.stdout, indent=4)
        print()

    ref_repo_tmp_dir.cleanup()
    cmp_repo_tmp_dir.cleanup()


def diff_tags(ref_repo: str, ref_tag: str, cmp_tag: str, is_posix_diff: bool):
    """
    Perform a diff between two tags in the same repository.

    Args:
        ref_repo (str): The reference repository URL.
        ref_tag (str): The reference tag name.
        cmp_tag (str): The tag to compare against.
    """

    ref_repo_tmp_dir = sparse_branch_checkout(ref_repo, ref_tag)
    cmp_repo_tmp_dir = sparse_branch_checkout(ref_repo, cmp_tag)

    final_result = diff_config(Path(ref_repo_tmp_dir.name), Path(cmp_repo_tmp_dir.name))

    if is_posix_diff:
        # Print the diff in POSIX format
        print_diff_posix_format(final_result)
    else:
        json.dump(final_result, sys.stdout, indent=4)
        print()

    ref_repo_tmp_dir.cleanup()
    cmp_repo_tmp_dir.cleanup()


def diff_branch_remote_repo(
    ref_repo: str, ref_branch: str, remote_repo: str, cmp_branch: str, is_posix_diff: bool
):
    """
    Perform a diff between branches in different repositories.

    Args:
        ref_repo (str): The reference repository URL.
        ref_branch (str): The reference branch name.
        remote_repo (str): The remote repository URL.
        cmp_branch (str): The branch to compare against.
    """
    ref_repo_tmp_dir = sparse_branch_checkout(ref_repo, ref_branch)
    cmp_repo_tmp_dir = sparse_branch_checkout(remote_repo, cmp_branch)

    final_result = diff_config(Path(ref_repo_tmp_dir.name), Path(cmp_repo_tmp_dir.name))

    if is_posix_diff:
        # Print the diff in POSIX format
        print_diff_posix_format(final_result)
    else:
        json.dump(final_result, sys.stdout, indent=4)
        print()

    ref_repo_tmp_dir.cleanup()
    cmp_repo_tmp_dir.cleanup()


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(
        dest="mode", help="the mode in which diff should be performed"
    )

    # diff-branch mode
    parser_diff_branch = subparsers.add_parser("diff-branch", help="diff between branches")
    parser_diff_branch.add_argument(
        "--ref-repo",
        nargs="?",
        default=CEPH_UPSTREAM_REMOTE_URL,
        help="the repository URL from where the reference config files will be fetched",
    )
    parser_diff_branch.add_argument("--ref-branch", required=True, help="the reference branch")
    parser_diff_branch.add_argument(
        "--cmp-branch", required=True, help="the branch to compare against reference"
    )
    parser_diff_branch.add_argument(
        "--posix-diff",
        action="store_true",
        help="output the configuration diff in POSIX diff like format",
    )

    # diff-tag mode
    parser_diff_tag = subparsers.add_parser("diff-tag", help="diff between tags")
    parser_diff_tag.add_argument(
        "--ref-repo",
        nargs="?",
        default=CEPH_UPSTREAM_REMOTE_URL,
        help="the repository URL from where the reference config files will be fetched",
    )
    parser_diff_tag.add_argument("--ref-tag", required=True, help="the reference tag version")
    parser_diff_tag.add_argument(
        "--cmp-tag", required=True, help="the tag version to compare against reference"
    )
    parser_diff_tag.add_argument(
        "--posix-diff",
        action="store_true",
        help="output the configuration diff in POSIX diff like format",
    )

    # diff-branch-remote-repo mode
    parser_diff_branch_remote_repo = subparsers.add_parser(
        "diff-branch-remote-repo", help="diff between branches in different repositories"
    )
    parser_diff_branch_remote_repo.add_argument(
        "--ref-repo",
        nargs="?",
        default=CEPH_UPSTREAM_REMOTE_URL,
        help="the repository URL from where the reference config files will be fetched",
    )
    parser_diff_branch_remote_repo.add_argument(
        "--remote-repo", required=True, help="the remote repository URL"
    )
    parser_diff_branch_remote_repo.add_argument(
        "--ref-branch", required=True, help="the reference branch"
    )
    parser_diff_branch_remote_repo.add_argument(
        "--cmp-branch", required=True, help="the branch to compare against"
    )
    parser_diff_branch_remote_repo.add_argument(
        "--posix-diff",
        action="store_true",
        help="output the configuration diff in POSIX diff like format",
    )

    args = parser.parse_args()

    if args.mode == "diff-branch":
        diff_branch(args.ref_repo, args.ref_branch, args.cmp_branch, args.posix_diff)

    elif args.mode == "diff-tag":
        diff_tags(args.ref_repo, args.ref_tag, args.cmp_tag, args.posix_diff)

    elif args.mode == "diff-branch-remote-repo":
        diff_branch_remote_repo(
            args.ref_repo, args.ref_branch, args.remote_repo, args.cmp_branch, args.posix_diff)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
