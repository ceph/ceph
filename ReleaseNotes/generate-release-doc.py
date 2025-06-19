#!/usr/bin/env python3
#
# Copyright 2025 IBM, Inc.
# SPDX-License-Identifier: LGPL-2.1-or-later
#
# This script was generated with the assistance of an AI language model.
#
# This is free software; you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License version 2.1, as published by
# the Free Software Foundation.  See file COPYING.

import argparse
import io
import os
import sys
import yaml
from pathlib import Path

# To use this script, you need to install PyYAML and docutils:
# pip install PyYAML docutils

try:
    from docutils.core import publish_string
    from docutils.utils import SystemMessage
except ImportError:
    print("Error: 'docutils' library not found. Please run 'pip install docutils'", file=sys.stderr)
    sys.exit(1)


def get_yaml_files(directory):
    """
    Finds all YAML files in a given directory (non-recursively).

    Args:
        directory (str or Path): The directory to search.

    Returns:
        list[Path]: A list of Path objects for each YAML file found.
    """
    path = Path(directory)
    if not path.is_dir():
        print(f"Error: Directory not found at '{directory}'", file=sys.stderr)
        return []
    # Supporting both .yml and .yaml extensions
    return list(path.glob('*.yaml')) + list(path.glob('*.yml'))

def get_all_yaml_files_in_paths(paths):
    """
    Finds all YAML files from a list of paths (files or directories).
    Directories are searched recursively.
    
    Args:
        paths (list[str]): A list of file or directory paths.
        
    Returns:
        list[Path]: A sorted list of unique Path objects for each YAML file.
    """
    all_files = set()
    for p_str in paths:
        p = Path(p_str)
        if p.is_dir():
            all_files.update(p.rglob('*.yaml'))
            all_files.update(p.rglob('*.yml'))
        elif p.is_file() and p.suffix in ['.yaml', '.yml']:
            all_files.add(p)
    return sorted(list(all_files))


def parse_yaml_file(file_path):
    """
    Parses a single YAML file.

    Args:
        file_path (Path): The path to the YAML file.

    Returns:
        dict: The parsed YAML content, or None if an error occurs.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except yaml.YAMLError as e:
        print(f"Error: Invalid YAML in '{file_path}':\n{e}", file=sys.stderr)
        return None
    except IOError as e:
        print(f"Error reading file '{file_path}': {e}", file=sys.stderr)
        return None

def render_rst(release_notes_data):
    """
    Renders the release notes data into a structured reStructuredText document.

    Each key in 'release_notes_data["section"]' corresponds to a section object
    with a 'name' and 'notes'. The 'footer' key is handled specially.

    Args:
        release_notes_data (dict): The combined release notes data.

    Returns:
        str: A formatted reStructuredText string.
    """
    if not release_notes_data:
        return ""

    output_parts = []
    section_underline = '-'
    
    sections = release_notes_data.get('section', {})
    section_keys = sorted(sections.keys())

    for key in section_keys:
        section_data = sections[key]
        if not isinstance(section_data, dict):
            continue
            
        items = section_data.get('notes', [])
        if not items:
            continue
            
        # Use the 'name' field for the title, fallback to the key
        title = section_data.get('name', key.replace('_', ' ').replace('-', ' '))
        header = f"{title}\n{section_underline * len(title)}"
        output_parts.append(header)

        # Create a bulleted list for the items in the section
        bullet_list = []
        if isinstance(items, list):
            for item in items:
                # Indent multi-line notes correctly for the bullet list
                lines = str(item).strip().split('\n')
                bullet_list.append(f"* {lines[0]}")
                for line in lines[1:]:
                    bullet_list.append(f"  {line}")
        else:
             # Handle non-list items gracefully, though lists are expected
             bullet_list.append(f"* {str(items).strip()}")

        output_parts.append("\n".join(bullet_list))

    # Handle the footer last, which is at the top level
    if 'footer' in release_notes_data:
        footer_content = release_notes_data['footer']
        if isinstance(footer_content, list):
            # Join list items, assuming they are complete RST snippets
            footer_text = "\n".join(str(item).strip() for item in footer_content)
            output_parts.append(footer_text)
        else:
            output_parts.append(str(footer_content).strip())

    return "\n\n".join(output_parts)

def deep_merge(dict1, dict2):
    """
    Recursively merges dict2 into dict1.
    - Concatenates lists.
    - Merges dicts recursively.
    - Overwrites other values (dict2 wins).
    """
    for key, value in dict2.items():
        if key in dict1 and isinstance(dict1.get(key), dict) and isinstance(value, dict):
            deep_merge(dict1[key], value)
        elif key in dict1 and isinstance(dict1.get(key), list) and isinstance(value, list):
            dict1[key].extend(value)
        else:
            dict1[key] = value
    return dict1

def generate_command(args):
    """Handler for the 'generate' command."""
    yaml_files = get_yaml_files(args.source_dir)
    if not yaml_files:
        print(f"No YAML files found in '{args.source_dir}'.", file=sys.stderr)
        return 1

    combined_data = {}
    for file_path in sorted(yaml_files):
        print(f"Processing '{file_path}'...", file=sys.stderr)
        content = parse_yaml_file(file_path)
        if content:
            deep_merge(combined_data, content)

    release_notes = combined_data.get('release-notes', {})

    if release_notes:
        final_rst = render_rst(release_notes)
        try:
            args.output_file.write(final_rst)
            if final_rst:
                args.output_file.write('\n')
        except IOError as e:
            print(f"Error writing to output: {e}", file=sys.stderr)
            return 1
        finally:
            if args.output_file is not sys.stdout:
                args.output_file.close()
                print(f"\nSuccessfully generated RST to '{args.output_file.name}'", file=sys.stderr)
    else:
        print("No 'release-notes' entries were found to generate.", file=sys.stderr)
    
    return 0

def validate_rst_notes(notes, file_path, section_name):
    """
    Helper function to validate a list of RST notes.
    It now captures the docutils error stream for more detailed output on failure.
    """
    is_valid = True
    note_list = notes if isinstance(notes, list) else [notes]
    for i, note in enumerate(note_list):
        # Create an in-memory stream to capture warnings and errors
        error_stream = io.StringIO()

        try:
            # Configure docutils to halt on a warning (level 2) and raise an exception.
            # The warning_stream is now our in-memory buffer.
            settings = {
                'halt_level': 1,
                'warning_stream': error_stream,
            }
            publish_string(
                source=str(note),
                writer_name='null',
                settings_overrides=settings
            )
        except SystemMessage as e:
            # On failure, retrieve the detailed error from the captured stream.
            error_details = error_stream.getvalue()

            print(f"Error: Invalid reStructuredText in '{file_path}' (section: '{section_name}', entry {i+1}):\n", file=sys.stderr)
            # Print the detailed error from docutils before the summary.
            if error_details:
                print("--- Docutils Details ---\n"
                      f"{error_details.strip()}\n"
                      "------------------------", file=sys.stderr)
            is_valid = False
        finally:
            # Ensure the stream is closed
            error_stream.close()

    return is_valid

def check_file(file_path):
    content = parse_yaml_file(file_path)
    if content is None:
        print(f"Info: invalid yaml: '{file_path}'.", file=sys.stderr)
        return False

    release_notes = content.get('release-notes', {})
    if not release_notes:
        print(f"Info: No 'release-notes' entries found in '{file_path}'.", file=sys.stderr)
        return False

    body = []

    header = release_notes.get('header', [])
    if header:
        body += header
        body.append('\n')

    sections = release_notes.get('section', {})
    for section_key, section_data in sections.items():
        if isinstance(section_data, dict):
            notes_to_validate = section_data.get('notes', [])
            body += notes_to_validate

    footer = release_notes.get('footer', [])
    body += footer

    body = "".join(body)
    if not validate_rst_notes(body, file_path, section_key):
        print(f"Info: 'notes' does not compile in '{file_path}'.", file=sys.stderr)
        return False

    return True

def check_command(args):
    """Handler for the 'check' command."""
    files_to_check = get_all_yaml_files_in_paths(args.paths)
    if not files_to_check:
        print("No YAML files found in the specified paths.", file=sys.stderr)
        return 1
        
    invalid_files = 0
    for file_path in files_to_check:
        print(f"Checking '{file_path}'...", file=sys.stderr)

        if check_file(file_path):
            print(f"OK: '{file_path}' is valid.", file=sys.stderr)
        else:
            invalid_files += 1
            print(f"FAILED: '{file_path}' has errors.", file=sys.stderr)

        print("-" * 20, file=sys.stderr)

    if invalid_files > 0:
        print(f"\nCheck finished. Found errors in {invalid_files} file(s).", file=sys.stderr)
        return 1
    else:
        print("\nCheck finished. All files are valid.", file=sys.stderr)
        return 0

def main():
    """Main function to drive the script."""
    parser = argparse.ArgumentParser(
        description="A tool for managing Ceph-style release note fragments.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    subparsers = parser.add_subparsers(dest='command', required=True, help="Available commands")

    # --- Generate Command ---
    parser_generate = subparsers.add_parser(
        'generate',
        help='Combine release note fragments and generate a final reStructuredText document.'
    )
    parser_generate.add_argument(
        'source_dir',
        nargs='?',
        default='next',
        help="The directory containing the release note YAML files. Defaults to 'next'."
    )
    parser_generate.add_argument(
        '-o', '--output',
        dest='output_file',
        type=argparse.FileType('w', encoding='utf-8'),
        default=sys.stdout,
        help="The output file to write the RST document to. Defaults to stdout."
    )
    parser_generate.set_defaults(func=generate_command)

    # --- Check Command ---
    parser_check = subparsers.add_parser(
        'check',
        help='Check that one or more fragment files are valid YAML and contain valid reStructuredText.'
    )
    parser_check.add_argument(
        'paths',
        nargs='+',
        help='One or more files or directories to validate. Directories will be searched recursively for .yaml/.yml files.'
    )
    parser_check.set_defaults(func=check_command)

    args = parser.parse_args()
    # Call the appropriate handler function and exit with its return code
    sys.exit(args.func(args))

if __name__ == "__main__":
    main()
