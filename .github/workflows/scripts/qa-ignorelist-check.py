#!/usr/bin/env python3
"""
Find the teuthology log-ignorelist entries a pull request adds, and
require the "QA log ignorelist" item of the PR checklist to be ticked
when there are any.

An ignorelist entry hides a cluster log warning from every job that
includes the yaml, and many of those yamls are shared between suites
through symlinks, so a new entry needs a review by the team that owns
those suites.

Reads GITHUB_TOKEN, GITHUB_REPOSITORY, PR_NUMBER and PR_BODY from the
environment. Only reads the PR's files through the API; it never runs
anything from the PR.
"""

import json
import os
import re
import sys
import urllib.error
import urllib.request

import yaml

API = 'https://api.github.com'
IGNORELIST_KEYS = ('log-ignorelist', 'log_ignorelist', 'log-whitelist')
SECTION = 'QA log ignorelist'

section_re = re.compile(r'^- ([^(]+)( \(.+)?$')
item_re = re.compile(r'^  - \[(.+)\] (.+)$')


def api(path, raw=False):
    req = urllib.request.Request(API + path)
    req.add_header('Authorization', 'Bearer ' + os.environ['GITHUB_TOKEN'])
    req.add_header('X-GitHub-Api-Version', '2022-11-28')
    req.add_header('Accept', 'application/vnd.github.raw+json' if raw
                   else 'application/vnd.github+json')
    with urllib.request.urlopen(req) as resp:
        data = resp.read()
    return data.decode() if raw else json.loads(data)


def changed_files(repo, pr):
    page = 1
    while True:
        files = api(f'/repos/{repo}/pulls/{pr}/files?per_page=100&page={page}')
        yield from files
        if len(files) < 100:
            return
        page += 1


def is_symlink(repo, path, ref, cache={}):
    # the contents API follows a symlink to a file, but a directory
    # listing tells them apart
    parent = path.rpartition('/')[0]
    if (parent, ref) not in cache:
        cache[parent, ref] = {e['path']: e['type'] for e in
                              api(f'/repos/{repo}/contents/{parent}?ref={ref}')}
    return cache[parent, ref].get(path) == 'symlink'


def content(repo, path, ref):
    try:
        return api(f'/repos/{repo}/contents/{path}?ref={ref}', raw=True)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def ignorelist_entries(text):
    """All strings found under an ignorelist key, at any depth."""
    entries = set()

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                if key in IGNORELIST_KEYS and isinstance(value, list):
                    entries.update(str(v) for v in value)
                else:
                    walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    try:
        for doc in yaml.safe_load_all(text):
            walk(doc)
    except yaml.YAMLError:
        # not ours to judge; teuthology will reject it
        pass
    return entries


def checklist_ticked(body):
    saw_checklist = False
    section = None
    for line in body.replace('\r\n', '\n').split('\n'):
        if line.strip() == '## Checklist':
            saw_checklist = True
            continue
        if not saw_checklist:
            continue
        if match := section_re.match(line):
            section = match.group(1).strip()
        elif match := item_re.match(line):
            if section == SECTION and match.group(1).strip().lower() == 'x':
                return True
    return False


def main():
    repo = os.environ['GITHUB_REPOSITORY']
    pr = os.environ['PR_NUMBER']
    pull = api(f'/repos/{repo}/pulls/{pr}')
    head = pull['head']['sha']
    # compare against the merge base, so entries that main dropped
    # after the PR branched do not look like additions
    base = api(f'/repos/{repo}/compare/{pull["base"]["sha"]}...{head}')[
        'merge_base_commit']['sha']

    added = {}
    for f in changed_files(repo, pr):
        path = f['filename']
        if not path.startswith('qa/') or not path.endswith(('.yaml', '.yml')):
            continue
        # a link to a shared yaml adds that yaml to one more suite, but the
        # entries in it were reviewed where the yaml itself was changed
        if f['status'] == 'removed' or is_symlink(repo, path, head):
            continue
        new_text = content(repo, path, head)
        if new_text is None:
            continue
        old_text = content(repo, f.get('previous_filename', path), base) or ''
        new = ignorelist_entries(new_text) - ignorelist_entries(old_text)
        if new:
            added[path] = sorted(new)

    if not added:
        print('No log-ignorelist entries added.')
        return 0

    print('This PR adds log-ignorelist entries:')
    for path, entries in added.items():
        print(f'  {path}')
        for entry in entries:
            print(f'    - {entry}')

    if checklist_ticked(os.environ.get('PR_BODY') or ''):
        print(f'The "{SECTION}" checklist item is ticked.')
        return 0
    print(f'Error: tick the "{SECTION}" item of the PR checklist, after the '
          'team that owns the affected suites has agreed that these warnings '
          'are expected. If the description predates that item, add it under '
          '"## Checklist":')
    print(f'- {SECTION}')
    print('  - [x] The team owning the affected suites agreed these warnings '
          'are expected')
    return 1


if __name__ == '__main__':
    sys.exit(main())
