#!/usr/bin/python3
"""Test to ensure remote suodable executables are audited.

This file can be used in one of two ways:
* as a "unit test" executed by pytest
* as a script that can report on or check expected remote executables

It is designed to act as a method of ensuring that the executables that we run
on remote nodes under sudo are explicitly known. The types defined in ssh.py
act as both a way to audit the commands (via this script) and that we don't
lose track of what can be run - by relying on mypy.

The unit test mode integrates into pytest for convenience, it really acts as a
static check that scans the source code of the cephadm mgr module.  NOTE: the
file's test script mode is sensitive to it's location in the source tree. If
files get moved this script may need to be updated.

The test asserts that the `EXPECTED` list matches all tools that may be
executed remotely under sudo.

This file can also be run as a script. When supplied with a directory or list
of files it will read the sources and report on all remote executables found.
When run with the `--check` option it performs the same job as the unit test
mode but outputs a report in a more human readable format.

If the commands the manager module can execute remotely change the `EXPECTED`
this must be updated to match to ensure the change is being done deliberately.
Any corresponding documentation should also be updated.

Note that ideally the EXPECTED list should shrink and not grow.  Any changes to
the list may cause administrators of the ceph cluster to have to make manual
changes to the system prior/during upgrade!
"""
import ast
import os
import pathlib
import sys

ssh_py = 'ssh.py'
serve_py = 'serve.py'

# IMPORTANT - please read the entire module docstring before changing
# the list below.
EXPECTED = [
    # - value | is_constant | filename -
    # constant executables
    ('/usr/bin/cephadm', True, serve_py),
    ('chmod', True, ssh_py),
    ('chown', True, ssh_py),
    ('ls', True, ssh_py),
    ('mkdir', True, ssh_py),
    ('mv', True, ssh_py),
    ('rm', True, ssh_py),
    ('sysctl', True, ssh_py),
    ('touch', True, ssh_py),
    ('true', True, ssh_py),
    ('which', True, serve_py),
    # variable executables
    ('python', False, serve_py),
]


def test_expected_remote_executables():
    import pytest

    if sys.version_info < (3, 8):
        pytest.skip("python 3.8 or later required")

    self_path = pathlib.Path(__file__).resolve()
    # test is sensitive to where it is in the source tree. it expects to be in
    # a tests directory under the cephadm mgr module. if stuff gets moved
    # around this test will likely start failing and need to be updated
    remote_exes = find_sudoable_exes_in_files(
        _file_paths([self_path.parent.parent])
    )
    unexpected, gone = diff_remote_exes(remote_exes)
    unexpected_msgs = gone_msgs = []
    if unexpected or gone:
        unexpected_msgs, gone_msgs = format_diff(
            unexpected, gone, remote_exes
        )
    assert not unexpected_msgs, unexpected_msgs
    assert not gone_msgs, gone_msgs


def _essential(v):
    """Convert a remote exe dict to a tuple with only the essential fields."""
    return (v["value"], v["is_constant"], v["filename"].split("/")[-1])


def _names(node):
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, ast.Attribute):
        vn = _names(node.value)
        return vn + [node.attr]
    if isinstance(node, ast.Call):
        return _names(node.func)
    if isinstance(node, ast.Constant):
        return [repr(node.value)]
    if isinstance(node, ast.JoinedStr):
        return [f"<JoinedStr: {node.values!r}>"]
    if isinstance(node, ast.Subscript):
        return [f"<Subscript: {node.value}{node.slice}>"]
    if isinstance(node, ast.BinOp):
        return [f"<BinaryOp: {_names(node.left)} {_names(node.op)} {_names(node.right)}"]
    if (
        isinstance(node, ast.Add)
        or isinstance(node, ast.Sub)
        or isinstance(node, ast.Mult)
        or isinstance(node, ast.Div)
        or isinstance(node, ast.FloorDiv)
        or isinstance(node, ast.Mod)
        or isinstance(node, ast.Pow)
        or isinstance(node, ast.LShift)
        or isinstance(node, ast.RShift)
        or isinstance(node, ast.ButOr)
        or isinstance(node, ast.BitXor)
        or isinstance(node, ast.BitAnd)
        or isinstance(node, ast.MatMult)
    ):
        return [repr(node)]
    raise ValueError(f"_names: unexpected type: {node}")


def _arg_kind(node):
    assert isinstance(node, ast.Call)
    assert len(node.args) == 1
    arg = node.args[0]
    if isinstance(arg, ast.Constant):
        return str(arg.value), True
    names = _names(arg)
    return ".".join(names), False


class ExecutableVisitor(ast.NodeVisitor):
    def __init__(self):
        super().__init__()
        self.remote_executables = []

    def visit_Call(self, node):
        names = _names(node)
        if names[-1] == 'RemoteExecutable':
            value, is_constant = _arg_kind(node)
            self.remote_executables.append(
                dict(value=value, is_constant=is_constant, lineno=node.lineno)
            )
        self.generic_visit(node)


def find_sudoable_exes(tree):
    ev = ExecutableVisitor()
    ev.visit(tree)
    return ev.remote_executables


def find_sudoable_exes_in_files(files):
    out = []
    for file in files:
        with open(file) as fh:
            source = fh.read()
        tree = ast.parse(source, fh.name)
        rmes = find_sudoable_exes(tree)
        for rme in rmes:
            rme['filename'] = str(file)
        out.extend(rmes)
    return out


def diff_remote_exes(remote_exes):
    expected = set(EXPECTED)
    current = {_essential(v) for v in remote_exes}
    unexpected = current - expected
    gone = expected - current
    return unexpected, gone


def format_diff(unexpected, gone, remote_exes):
    current = {_essential(v): v for v in remote_exes}
    unexpected_msgs = []
    for val, is_constant, fn in unexpected:
        vn = 'constant' if is_constant else 'variable'
        vq = repr(val) if is_constant else val
        # info is needed for full filename/linenumber and is only relevant for
        # found (unexpected) entries
        info = current[(val, is_constant, fn)]
        unexpected_msgs.append(
            f'{vn} {vq} in {info["filename"]}:{info["lineno"]} not tracked'
        )
    gone_msgs = []
    for val, is_constant, fn in gone:
        vn = 'constant' if is_constant else 'variable'
        vq = repr(val) if is_constant else val
        gone_msgs.append(f"{vn} {vq} expected in {fn} not found")
    return unexpected_msgs, gone_msgs


def report_remote_exes(remote_exes):
    for rme in remote_exes:
        clabel = 'CONSTANT' if rme["is_constant"] else "VARIABLE"
        print(
            "{clabel:10} {value:10} {filename}:{lineno}".format(
                clabel=clabel, **rme
            )
        )


def report_compare_remote_exes(remote_exes):
    import textwrap

    unexpected, gone = diff_remote_exes(remote_exes)
    if not (unexpected or gone):
        print('No issues detected')
        sys.exit(0)
    unexpected_msgs, gone_msgs = format_diff(unexpected, gone, remote_exes)
    if unexpected_msgs:
        desc = textwrap.wrap(
            "One or more remote executable has been detected in the source"
            " files that is not tracked in the test. If this change is"
            " intended you must update the test AND update any corresponding"
            " documentation.",
            76,
        )
        for line in desc:
            print(line)
        print('-' * 76)
    for msg in unexpected_msgs:
        print(f'* {msg}')
    if unexpected_msgs:
        print()
    if gone_msgs:
        desc = textwrap.wrap(
            "One or more remote executable that is expected to appear"
            " in the source files has not been detected."
            " If this change is intended you must update the test AND update"
            " any corresponding documentation.",
            76,
        )
        for line in desc:
            print(line)
        print('-' * 76)
    for msg in gone_msgs:
        print(f'* {msg}')

    sys.exit(1)


def _file_paths(src_paths):
    files = set()
    for path in src_paths:
        if path.is_file():
            files.add(path)
            continue
        for d, ds, fs in os.walk(path):
            if 'tests' in ds:
                ds.remove('tests')
            dpath = pathlib.Path(d)
            for fn in fs:
                if fn.endswith('.py'):
                    files.add(dpath / fn)
    return files


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--check', action='store_true')
    parser.add_argument('PATH', nargs='+', type=pathlib.Path)
    cli = parser.parse_args()

    remote_exes = find_sudoable_exes_in_files(_file_paths(cli.PATH))
    if cli.check:
        report_compare_remote_exes(remote_exes)
    else:
        report_remote_exes(remote_exes)


if __name__ == '__main__':
    main()
