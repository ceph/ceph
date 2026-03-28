#!/usr/bin/env python3

"""
ceph_codechecker.py
-------------------

Wrapper script to run CodeChecker analyzers against a Ceph build.

This script prepares small configuration snippets (skip lists and
analyzer option files), invokes CodeChecker to analyze a compilation
database (compile_commands.json), parses the resulting plist files into
the requested export format, and optionally stores the result to a
CodeChecker server.

Key features:
- Prepares skipfile, cppcheck, clang-tidy and clang static analyzer
    option files next to the provided output directory.
- Supports analysis modes: 'fast', 'full', 'clangsa', 'cppcheck', and
    'clang-tidy'.
- Exports results in formats: html, json, codeclimate, gerrit, or
    baseline and can upload to a running CodeChecker server via --url.

Usage (short):
    ceph_codechecker.py [--verbose {info,debug_analyzer,debug}] \
                                            [--codechecker {full,fast,clangsa,cppcheck,clang-tidy}] \
                                            [--export {html,json,codeclimate,gerrit,baseline}] \
                                            [--url URL] [-o OUTPUT_DIR] COMPILE_COMMANDS

Requirements:
- Python 3
- CodeChecker installed in the environment (pip3 install codechecker)
- cppcheck:
  - RPM-based distros (Fedora/RHEL/CentOS): sudo dnf install cppcheck
  - DEB-based distros (Debian/Ubuntu): sudo apt-get install cppcheck
- clang-tidy:
  - RPM-based distros (Fedora/RHEL/CentOS): sudo dnf install clang-tools-extra
  - DEB-based distros (Debian/Ubuntu): sudo apt-get install clang-tidy
- Clang Static Analyzer:
  - RPM-based distros (Fedora/RHEL/CentOS): sudo dnf install clang-analyzer
  - DEB-based distros (Debian/Ubuntu): sudo apt-get install clang-tools
- Create compile_commands.py file:
  - Run './do_cmake.sh -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_C_COMPILER=/usr/bin/clang -DCMAKE_CXX_COMPILER=/usr/bin/clang++' to generate a compilation database.
  - Run 'cmake --build build' to compile the project.
  - Run './src/script/ceph_codechecker.py --codechecker [full|fast|clangsa|cppcheck|clang-tidy] -o ./build ./compile_commands.json' to analyze the project
    database to the output directory.
Notes:
- The script intentionally unsets OPENSSL_CONF for subprocesses to
    avoid OpenSSL md5 configuration issues when running CodeChecker.
"""

import copy
import os
import argparse
import datetime
import subprocess
import sys


def search_plist_files(search_dir):
    result = []
    if os.path.isdir(search_dir):
        for root, dirs, files in os.walk(search_dir):
            for file in files:
                filename, file_extension = os.path.splitext(file)
                if file_extension == ".plist":
                    result.extend([os.path.join(root, file)])
    return result


def build_codechecker(
    compile_commands: str,
    workspace_path: str,
    analysis_type: str,
    export: str,
    url: str,
    verbose: str,
    include_boost: bool,
):
    print("Building with codechecker")
    output_dir = f"csa-output-{os.environ.get('USER', 'unknown')}-{datetime.datetime.today().strftime('%Y%m%d-%H%M%S')}"

    environ = copy.deepcopy(os.environ)

    # Ensure the workspace_path directory exists
    os.makedirs(workspace_path, exist_ok=True)

    with open(os.path.join(workspace_path, "skipfile.cfg"), "w") as text_file:
        if not include_boost:
            text_file.write("-*/build/boost/*\n")
        text_file.write("-*/build/_deps/*\n")

    with open(os.path.join(workspace_path, "cppcheck.cfg"), "w") as text_file:
        text_file.write("--check-level=exhaustive\n-D__clang_analyzer__\n")

    with open(os.path.join(workspace_path, "tidyargs.cfg"), "w") as text_file:
        text_file.write("-p " + compile_commands)

    with open(os.path.join(workspace_path, "clangsa.cfg"), "w") as text_file:
        text_file.write("-Xclang\n-analyzer-disable-checker=deadcode.DeadStores\n")

    codechecker_default_options = [
        # severity: LOW, CRITICAL, MEDIUM, UNSPECIFIED, HIGH, STYLE
        "--disable",
        "severity:LOW",
        "--disable",
        "severity:STYLE",
        "-i",
        os.path.join(workspace_path, "skipfile.cfg"),
    ]

    codechecker_cppcheck_options = [
        "--analyzer-config",
        f"cppcheck:cc-verbatim-args-file={os.path.join(workspace_path, 'cppcheck.cfg')}",
    ]

    codechecker_clangtidy_options = [
        "--analyzer-config",
        f"clang-tidy:cc-verbatim-args-file={os.path.join(workspace_path, 'tidyargs.cfg')}",
        "--disable",
        "bugprone-unused-return-value",
        "--disable",
        "cert-err33-c",
        "--disable",
        "clang-diagnostic-unused-parameter",
        "--disable",
        "clang-diagnostic-double-promotion",
        "--disable",
        "clang-diagnostic-mismatched-tags",
        "--disable",
        "clang-diagnostic-float-conversion",
        "--disable",
        "clang-diagnostic-reserved-identifier",
        "--disable",
        "clang-diagnostic-reserved-macro-identifier",
        "--disable",
        "clang-diagnostic-unused-private-field",
        "--disable",
        "misc-confusable-identifiers",
    ]

    codechecker_clangsa_options = [
        "--analyzer-config",
        f"clangsa:cc-verbatim-args-file={os.path.join(workspace_path, 'clangsa.cfg')}",
    ]

    codechecker_fast_options = [
        "-e",
        "alpha.cplusplus",
        "-e",
        "alpha.core.BoolAssignment",
        "-e",
        "alpha.core.CastSize",
        "-e",
        "alpha.security",
        "-e",
        "core",
        "-e",
        "cplusplus",
        "-e",
        "unix",
        "-d",
        "optin",
        "-d",
        "optin.cplusplus",
        "-d",
        "nullability",
        "-d",
        "valist",
        "-d",
        "alpha.security",
        "-d",
        "deadcode.DeadStores",
        "--z3",
        "off",
        "--z3-refutation",
        "off",
        "--analyze-headers",
        "off",
        "--expand-macros",
        "off",
    ]
    # Unset OPENSSL_CONF to avoid issues with OpenSSL configuration in subprocesses
    environ["OPENSSL_CONF"] = ""
    codechecker_cmd = ["CodeChecker", "analyze"]

    if verbose:
        codechecker_cmd.extend(["--verbose", verbose])

    codechecker_cmd.extend(codechecker_default_options)

    if analysis_type == "fast":
        codechecker_cmd.extend(codechecker_fast_options)
        codechecker_cmd.extend(["--analyzers", "clangsa"])
        codechecker_cmd.extend(["--analyze-headers", "off", "--expand-macros", "off"])
    elif analysis_type == "full":
        codechecker_cmd.extend(["--analyzers", "clang-tidy", "cppcheck", "clangsa"])
        codechecker_cmd.extend(codechecker_clangtidy_options)
        codechecker_cmd.extend(codechecker_cppcheck_options)
        codechecker_cmd.extend(codechecker_clangsa_options)
    elif analysis_type == "clangsa":
        codechecker_cmd.extend(["--analyzers", "clangsa"])
        codechecker_cmd.extend(codechecker_clangsa_options)
    elif analysis_type == "cppcheck":
        codechecker_cmd.extend(["--analyzers", "cppcheck"])
        codechecker_cmd.extend(codechecker_cppcheck_options)
    elif analysis_type == "clang-tidy":
        codechecker_cmd.extend(["--analyzers", "clang-tidy"])
        codechecker_cmd.extend(codechecker_clangtidy_options)

    codechecker_cmd.extend(
        [
            "-o",
            os.path.join(workspace_path, output_dir),
            compile_commands,
        ]
    )

    try:
        print('Running "' + " ".join(codechecker_cmd) + '"')
        subprocess.run(codechecker_cmd, check=True, text=True, env=environ)
    except subprocess.CalledProcessError as e:
        if e.returncode == 3:
            pass
        else:
            raise e

    plist_files = search_plist_files(os.path.join(workspace_path, output_dir))

    codechecker_cmd = [
        "CodeChecker",
        "parse",
        *plist_files,
        "-e",
        f"{export}",
        "-o",
        os.path.join(workspace_path, output_dir),
    ]
    try:
        print('Running "' + " ".join(codechecker_cmd) + '"')
        subprocess.run(codechecker_cmd, check=True, text=True, env=environ)
    except subprocess.CalledProcessError as e:
        if e.returncode == 2:
            pass
        else:
            raise e

    if url:
        codechecker_cmd = [
            "CodeChecker",
            "store",
            os.path.join(workspace_path, output_dir),
            "-n",
            output_dir,
            "--url",
            url,
        ]
        try:
            print('Running "' + " ".join(codechecker_cmd) + '"')
            subprocess.run(codechecker_cmd, check=True, text=True, env=environ)
        except subprocess.CalledProcessError as e:
            if e.returncode == 2:
                pass
            else:
                raise e


#
# Start the script
#
if __name__ == "__main__":
    # Usage must be:
    # "ceph_codechecker.py  [--verbose] [--codechecker <type>] [--url <url>] [--export <format>]"
    # Optional arguments include:
    # --verbose (-v): Enable verbose reporting.
    # --codechecker: Enable codechecker analysis with options like 'full', 'fast', 'clangsa', 'cppcheck', or 'clang-tidy'.
    # --url: Upload codechecker report to a specified URL.
    # --export (-e): Specify extra output format type (e.g., 'html', 'json', 'codeclimate', etc.).

    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument(
        "--verbose",
        type=str,
        dest="verbose",
        choices=["info", "debug_analyzer", "debug"],
        help="Set verbosity level. If the value is 'debug' or "
        "'debug_analyzer' it will create a "
        "'codechecker.logger.debug' debug log file "
        "beside the given output file. It will contain "
        "debug information of compilation database "
        "generation. You can override the location of "
        "this file if you set the 'CC_LOGGER_DEBUG_FILE' "
        "environment variable to a different file path.",
    )

    arg_parser.add_argument(
        "--codechecker",
        type=str,
        choices=["full", "fast", "clangsa", "cppcheck", "clang-tidy"],
        help="Enable codechecker analysis",
    )
    arg_parser.add_argument(
        "--include-boost",
        action="store_true",
        default=False,
        help="Include boost headers in the analysis (by default boost headers are skipped)",
    )
    arg_parser.add_argument(
        "--url",
        type=str,
        default=None,
        help=(
            "Upload codechecker report to url, for example "
            + '"http://codechecker.ceph.org:8001/Default"'
        ),
    )
    arg_parser.add_argument(
        "--export",
        "-e",
        type=str,
        default="html",
        choices=["html", "json", "codeclimate", "gerrit", "baseline"],
        help="""
Specify extra output format type.
'codeclimate' format can be used for Code Climate and for GitLab integration. For more information see:
https://github.com/codeclimate/platform/blob/master/spec/analyzers/SPEC.md#data-types
'baseline' output can be used to integrate CodeChecker into your local workflow without using a CodeChecker server.
""",
    )

    arg_parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=os.path.abspath("codechecker_output"),
        help="Base output directory path (default: ./codechecker_output). The actual results will be written to a subdirectory named 'csa-output-<user>-<timestamp>' inside this directory.",
    )
    arg_parser.add_argument(
        "compile_commands",
        type=str,
        nargs="?",
        metavar="COMPILE_COMMANDS",
        help="(Required) Path to compile_commands.json file generated by your build system. This file is needed for CodeChecker analysis.",
    )
    args = arg_parser.parse_args()
    if not args.compile_commands:
        print(
            "Error: Path to compile_commands.json must be provided as a positional argument."
        )
        sys.exit(1)

    build_codechecker(
        compile_commands=args.compile_commands,
        workspace_path=args.output,
        analysis_type=args.codechecker,
        export=args.export,
        url=args.url,
        verbose=args.verbose,
        include_boost=args.include_boost,
    )
