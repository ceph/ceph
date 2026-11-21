#!/usr/bin/env python3
"""
Flag informal inter-module access in ceph-mgr modules:

* remote-unexported: remote('<module>', '<method>') calls a method that
  no @remote_export in that module's package, or in mgr_module.py, marks.
* cross-module-kv: a config-key command on a literal 'mgr/<other>/' key,
  i.e. another module's KV store.
* own-kv-via-mon: 'config-key get' of the module's own 'mgr/<module>/' key
  through mon_command, which get_store() serves from the local cache.

Only literal arguments are checked. Findings listed in the baseline file
are reported as known and don't fail the run, so only new ones do.
"""

import argparse
import ast
import sys
from pathlib import Path
from typing import Dict, Iterator, Optional, Set, Tuple

Finding = Tuple[str, str, str]  # (path, check, detail)


def _const_str(node: Optional[ast.AST]) -> Optional[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _call_name(node: ast.Call) -> Optional[str]:
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    if isinstance(node.func, ast.Name):
        return node.func.id
    return None


def _dict_get(node: ast.Dict, key: str) -> Optional[ast.AST]:
    for k, v in zip(node.keys, node.values):
        if _const_str(k) == key:
            return v
    return None


def _exported_names(tree: ast.AST) -> Iterator[str]:
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for d in node.decorator_list:
                d_name = d.attr if isinstance(d, ast.Attribute) else getattr(d, 'id', None)
                if d_name == 'remote_export':
                    yield node.name


def check_file(path: Path, root: Path,
               exports: Dict[Optional[str], Set[str]]) -> Iterator[Finding]:
    rel = path.relative_to(root)
    owner = rel.parts[0] if len(rel.parts) > 1 else None
    tree = ast.parse(path.read_text(), str(path))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = _call_name(node)
        if name == 'remote' and len(node.args) >= 2:
            module = _const_str(node.args[0])
            method = _const_str(node.args[1])
            if (module and method and method not in exports[None]
                    and method not in exports.get(module, set())):
                yield (str(rel), 'remote-unexported', f'{module}.{method}')
        elif name in ('mon_command', 'check_mon_command') and node.args:
            cmd = node.args[0]
            if not isinstance(cmd, ast.Dict):
                continue
            prefix = _const_str(_dict_get(cmd, 'prefix')) or ''
            key = _const_str(_dict_get(cmd, 'key')) or ''
            if not prefix.startswith('config-key') or not key.startswith('mgr/'):
                continue
            target = key.split('/')[1]
            if target != owner:
                yield (str(rel), 'cross-module-kv', key)
            elif prefix == 'config-key get':
                yield (str(rel), 'own-kv-via-mon', key)


def load_baseline(path: Optional[Path]) -> Set[Finding]:
    known: Set[Finding] = set()
    if path is None or not path.exists():
        return known
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#'):
            p, check, detail = line.split(maxsplit=2)
            known.add((p, check, detail))
    return known


def main() -> int:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument('--root', type=Path, default=here)
    parser.add_argument('--baseline', type=Path,
                        default=here / 'check_remote_calls.baseline')
    parser.add_argument('--update-baseline', action='store_true',
                        help='rewrite the baseline with the current findings')
    args = parser.parse_args()

    root = args.root.resolve()
    paths = [p for p in sorted(root.rglob('*.py'))
             if not ({'tests', '.tox', 'node_modules'} & set(p.relative_to(root).parts))]
    # package name -> exported names; None holds the MgrModule base ones
    exports: Dict[Optional[str], Set[str]] = {}
    for path in paths:
        rel = path.relative_to(root)
        pkg = rel.parts[0] if len(rel.parts) > 1 else None
        if pkg is None and rel.name != 'mgr_module.py':
            continue
        names = set(_exported_names(ast.parse(path.read_text(), str(path))))
        exports.setdefault(pkg, set()).update(names)
    exports.setdefault(None, set())
    findings = sorted({f for path in paths for f in check_file(path, root, exports)})

    if args.update_baseline:
        with open(args.baseline, 'w') as out:
            out.write('# Known findings of check_remote_calls.py: '
                      'fix them, don\'t add to them.\n')
            for f in findings:
                out.write(' '.join(f) + '\n')
        return 0

    known = load_baseline(args.baseline)
    new = [f for f in findings if f not in known]
    for f in new:
        print(f'{f[0]}: {f[1]}: {f[2]}')
    fixed = known - set(findings)
    if fixed:
        print(f'{len(fixed)} baseline entries no longer found, '
              'run with --update-baseline', file=sys.stderr)
    return 1 if new else 0


if __name__ == '__main__':
    sys.exit(main())
