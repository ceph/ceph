#!/usr/bin/python3

# Copyright 2025 IBM, Inc.
# SPDX-License-Identifier: LGPL-2.1-or-later
#
# This script was generated with the assistance of an AI language model.
#
# This is free software; you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License version 2.1, as published by
# the Free Software Foundation.  See file COPYING.

import argparse
import copy
import functools
import inspect
import itertools
import json
import logging
import os
import random
import re
import signal
import sys
import textwrap
import traceback

from datetime import datetime, timedelta, timezone
from getpass import getuser
from os.path import expanduser

import git # https://github.com/gitpython-developers/gitpython
import redminelib # https://pypi.org/project/python-redmine/
import requests

GITHUB_TOKEN = None
try:
    with open(expanduser("~/.github_token")) as f:
        GITHUB_TOKEN = f.read().strip()
except FileNotFoundError:
    pass
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", GITHUB_TOKEN)

GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == 'true'
GITHUB_SERVER_URL = os.getenv("GITHUB_SERVER_URL", "https://github.com")
GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "ceph/ceph")
GITHUB_RUN_ID = os.getenv("GITHUB_RUN_ID", "nil")

GITHUB_ACTION_LOG = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/actions/runs/{GITHUB_RUN_ID}"

GITHUB_API_ENDPOINT = f"https://api.github.com/repos/{GITHUB_REPOSITORY}"

REDMINE_CUSTOM_FIELD_ID_BACKPORT = 2
REDMINE_CUSTOM_FIELD_ID_RELEASE = 16
REDMINE_CUSTOM_FIELD_ID_PULL_REQUEST_ID = 21
REDMINE_CUSTOM_FIELD_ID_TAGS = 31
REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT = 33
REDMINE_CUSTOM_FIELD_ID_FIXED_IN = 34
REDMINE_CUSTOM_FIELD_ID_RELEASED_IN = 35
REDMINE_CUSTOM_FIELD_ID_UPKEEP_TIMESTAMP = 37

# Open
REDMINE_STATUS_ID_NEW = 1
REDMINE_STATUS_ID_INPROGRESS = 2
REDMINE_STATUS_ID_TRIAGED = 18
REDMINE_STATUS_ID_NEEDINFO = 11
REDMINE_STATUS_ID_FIX_UNDER_REVIEW = 13
REDMINE_STATUS_ID_PENDING_BACKPORT = 14

# Closed
REDMINE_STATUS_ID_RESOLVED = 3
REDMINE_STATUS_ID_CLOSED  = 5
REDMINE_STATUS_ID_REJECTED = 6
REDMINE_STATUS_ID_WONTFIX = 8
REDMINE_STATUS_ID_CANTREPRODUCE = 9
REDMINE_STATUS_ID_DUPLICATE = 10
REDMINE_STATUS_ID_WONTFIX_EOL = 19

REDMINE_TRACKER_ID_BACKPORT = 9

REDMINE_STATUS_ID_PENDING_BACKPORT = 14
REDMINE_STATUS_ID_RESOLVED = 3

REDMINE_ENDPOINT = "https://tracker.ceph.com"
REDMINE_API_KEY = None
try:
    with open(expanduser("~/.redmine_key")) as f:
        REDMINE_API_KEY = f.read().strip()
except FileNotFoundError:
    pass
REDMINE_API_KEY = os.getenv("REDMINE_API_KEY", REDMINE_API_KEY)

# Global flag for GitHub Actions output format
IS_GITHUB_ACTION = os.getenv("GITHUB_ACTION") is not None

class IssueLoggerAdapter(logging.LoggerAdapter):
    """
    A logging adapter that adds issue ID context to log messages.
    For GitHub Actions, it also handles grouping and error annotations.
    """
    def process(self, msg, kwargs):
        issue_id = int(self.extra['issue_id'])
        transform_name = self.extra['current_transform']
        if IS_GITHUB_ACTION:
            if transform_name:
                msg = f"[{transform_name}] {msg}"
            # Handle error annotations
            if self.logger.level == logging.ERROR or self.logger.level == logging.CRITICAL:
                return f"::error::{msg}", kwargs
        else:
            if transform_name:
                msg = f"[Issue #{issue_id} => {transform_name}] {msg}"
            else:
                msg = f"[Issue #{issue_id}] {msg}"
        return msg, kwargs

log = logging.getLogger(__name__)
log_stream = logging.StreamHandler()
log.addHandler(log_stream)
log.setLevel(logging.INFO)

GITHUB_HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "X-GitHub-Api-Version": "2022-11-28",
}

def post_github_comment(session, pr_id, body):
    """Helper to post a comment to a GitHub PR."""
    if RedmineUpkeep.GITHUB_RATE_LIMITED:
        log.warning("GitHub API rate limit hit previously. Skipping posting comment.")
        return False

    log.info(f"Posting a comment to GitHub PR #{pr_id}.")
    endpoint = f"{GITHUB_API_ENDPOINT}/issues/{pr_id}/comments"
    payload = {'body': body}
    try:
        response = session.post(endpoint, headers=GITHUB_HEADERS, json=payload)
        response.raise_for_status()
        log.info(f"Successfully posted comment to PR #{pr_id}.")
        return True
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403 and "rate limit exceeded" in e.response.text:
            log.error(f"GitHub API rate limit exceeded when commenting on PR #{pr_id}.")
            RedmineUpkeep.GITHUB_RATE_LIMITED = True
        else:
            log.error(f"GitHub API error posting comment to PR #{pr_id}: {e} - Response: {e.response.text}")
        return False
    except requests.exceptions.RequestException as e:
        log.error(f"Network or request error posting comment to GitHub PR #{pr_id}: {e}")
        return False


class UpkeepException(Exception):
    def __init__(self, issue_update, exception=None, traceback=None):
        self.issue_update = issue_update
        self.exception = exception
        self.traceback = traceback

    def comment(self):
        raise NotImplementedError()

class PRInvalidException(UpkeepException):
    def __init__(self, issue_update, pr_id, **kwargs):
        super().__init__(issue_update, **kwargs)
        self.pr_id = pr_id

    def __str__(self):
        return "PR is invalid"

    def comment(self):
        return f"""
Issue #{self.issue_update.issue.id} referenced "PR #{self.pr_id}":https://github.com/ceph/ceph/pull/{self.pr_id} is invalid:

<pre>
{self.traceback.strip()}
</pre>
"""

class PRClosedException(UpkeepException):
    def __init__(self, issue_update, pr_id, **kwargs):
        super().__init__(issue_update, **kwargs)
        self.pr_id = pr_id

    def __str__(self):
        return "PR is closed without merge"

    def comment(self):
        return f"""
Issue #{self.issue_update.issue.id} with status {self.issue_update.issue.status.name} references "PR #{self.pr_id}":https://github.com/ceph/ceph/pull/{self.pr_id} which is closed but not merged.

Possible resolutions:

* **If the PR id is wrong, please update it.**
* **If the issue was fixed through other means (e.g. in the kernel or Rook), please remove the PR id.**
* **If the PR is already merged through other means (erroneous backport), mark the issue state as "Rejected".**
* **Do nothing. This script will ignore this issue while the upkeep-failed tag is applied.**
"""

class RedmineUpdateException(UpkeepException):
    def __init__(self, issue_update, **kwargs):
        super().__init__(issue_update, **kwargs)

    def __str__(self):
        return "Update to Redmine failed"

    def comment(self):
        return f"""
Redmine Update failed:

<pre>
{self.traceback.strip()}
</pre>
"""


class IssueUpdate:
    def __init__(self, issue, github_session, git_repo):
        self.issue = issue
        self.update_payload = {}
        self.github_session = github_session
        self.git_repo = git_repo
        self._pr_cache = {}
        self.has_changes = False # New flag to track if changes are made
        self.transform = None
        logger_extra = {
          'issue_id': issue.id,
          'current_transform': None,
        }
        self.logger = IssueLoggerAdapter(logging.getLogger(__name__), extra=logger_extra)

    def set_transform(self, transform):
        self.transform = transform
        self.logger.extra['current_transform'] = transform

    def get_raw_custom_field(self, field_id):
        cf = self.issue.custom_fields.get(field_id)
        try:
            return cf.value if cf else None
        except redminelib.exceptions.ResourceAttrError:
            return None

    def get_custom_field(self, field_id):
        """ Get the custom field, first from update_payload otherwise issue """
        custom_fields = self.update_payload.setdefault("custom_fields", [])
        for field in custom_fields:
            if field.get('id') == field_id:
                return field['value']
        return self.get_raw_custom_field(field_id)

    def add_or_update_custom_field(self, field_id, value):
        """Helper to add or update a custom field in the payload."""
        custom_fields = self.update_payload.setdefault("custom_fields", [])
        found = False
        current_value = self.get_custom_field(field_id) # Get current value from issue or payload

        if current_value == value:
            # Value is already the same, no change needed
            self.logger.debug(f"Field {field_id} is already set to '{value}'. No update needed.")
            return False

        self.logger.debug(f"Updating custom field {field_id} from '{current_value}' to '{value}'.")
        for field in custom_fields:
            if field.get('id') == field_id:
                field['value'] = value
                found = True
                break
        if not found:
            custom_fields.append({'id': field_id, 'value': value})
        self.has_changes = True # Mark that a change has been made
        return True

    def change_field(self, field, value):
        self.logger.debug(f"Changing field '{field}' to '{value}'.")
        if self.update_payload.get(field) == value:
            return False
        else:
            self.update_payload[field] = value
            self.has_changes = True
            return True

    def add_tag(self, tag):
        current_tags_str = self.get_custom_field(REDMINE_CUSTOM_FIELD_ID_TAGS)
        current_tags = []
        if current_tags_str:
            current_tags = [current_tag.strip() for current_tag in current_tags_str.split(',') if current_tag.strip()]

        if tag in current_tags:
            self.logger.debug(f"tag '{tag}' already in tags")
            return
        else:
            current_tags.append(tag)
            self.logger.info(f"Adding '{tag}' tag.")

        new_tags = ", ".join(current_tags)
        self.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_TAGS, new_tags)

    def get_update_payload(self, suppress_mail=True): # Added suppress_mail parameter
        today = datetime.now(timezone.utc).isoformat(timespec='seconds')
        self.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_UPKEEP_TIMESTAMP, today)

        current_tags_str = self.get_custom_field(REDMINE_CUSTOM_FIELD_ID_TAGS)
        current_tags = []
        if current_tags_str:
            current_tags = [tag.strip() for tag in current_tags_str.split(',') if tag.strip()]
            if "upkeep-failed" in current_tags:
                self.logger.info(f"'upkeep-failed' tag found in '{current_tags_str}'. Removing for update.")
                current_tags.remove("upkeep-failed")
                self.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_TAGS, ", ".join(current_tags))

        payload = {
            'issue': self.update_payload,
        }
        if suppress_mail:
            payload['suppress_mail'] = "1"
        return payload

    def get_pr_id(self):
        self.logger.debug("Attempting to fetch PR data.")
        pr_id = self.get_custom_field(REDMINE_CUSTOM_FIELD_ID_PULL_REQUEST_ID)
        if not pr_id:
            self.logger.warning("No PR ID found in 'Pull Request ID'.")

            # If not found in custom field, try to find it in the issue description
            if self.issue.tracker.id == REDMINE_TRACKER_ID_BACKPORT and self.issue.description:
                self.logger.debug("Checking issue description for PR link.")
                match = re.search(r'^https://github.com/ceph/ceph/pull/(\d+)$', self.issue.description.strip())
                if match:
                    pr_id = match.group(1)
                    self.logger.info("Found PR ID #%s in issue description.", pr_id)

        try:
            return int(pr_id)
        except (ValueError, TypeError): # Handle None or non-integer values
            self.logger.warning(f"Invalid or missing PR ID '{pr_id}'. Cannot fetch PR.")
            return None

    def fetch_pr(self):
        pr_id = self.get_pr_id()

        # Check if rate limit has been hit globally
        if RedmineUpkeep.GITHUB_RATE_LIMITED:
            self.logger.warning("GitHub API rate limit hit previously. Skipping PR fetch.")
            return None

        if pr_id in self._pr_cache:
            self.logger.debug("Found PR #%d in cache.", pr_id)
            return self._pr_cache[pr_id]

        self.logger.info("Fetching PR #%d from GitHub API.", pr_id)
        endpoint = f"{GITHUB_API_ENDPOINT}/pulls/{pr_id}"
        params = {}
        try:
            response = self.github_session.get(endpoint, headers=GITHUB_HEADERS, params=params)
            response.raise_for_status()
            pr_data = response.json()
            self.logger.debug("PR #%d json:\n%s", pr_id, pr_data)
            # If we got the PR number through other means, update the field:
            self.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_PULL_REQUEST_ID, str(pr_id))
            self._pr_cache[pr_id] = pr_data
            return pr_data
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                self.logger.warning(f"GitHub PR #{pr_id} not found (404).")
                raise PRInvalidException(self, pr_id, exception=e, traceback=traceback.format_exc())
            elif response.status_code == 403 and "rate limit exceeded" in response.text:
                self.logger.error(f"GitHub API rate limit exceeded for PR #{pr_id}. Further GitHub API calls will be skipped.")
                RedmineUpkeep.GITHUB_RATE_LIMITED = True  # Set the global flag
            else:
                self.logger.error(f"GitHub API error for PR #{pr_id}: {e} - Response: {response.text}")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network or request error fetching GitHub PR #{pr_id}: {e}")
            return None

    def get_released(self, commit):
        """
        Determines the release version a commit is part of.
        """
        self.logger.debug(f"Checking release status for commit {commit}")
        try:
            release = self.git_repo.git.describe('--contains', '--match', 'v*.2.*', commit)
            self.logger.info("Commit %s is contained in git describe output: %s", commit, release)
            patt = r"v(\d+)\.(\d+)\.(\d+)"
            match = re.search(patt, release)
            if not match:
                self.logger.warning("Release '%s' is in invalid form, pattern mismatch.", release)
                return None
            if int(match.group(2)) != 2:
                self.logger.warning("Release '%s' is not a valid release (minor version not 2)", release)
                return None
            self.logger.info("Found valid release: %s", release)
            return release
        except git.exc.GitCommandError:
            self.logger.info("Commit %s not found in any matching release tag.", commit)
            return None

class RedmineUpkeep:
    # Class-level flag to track GitHub API rate limit status
    GITHUB_RATE_LIMITED = False
    MAX_UPKEEP_FAILURES = 5

    class Filter:
        PRIORITY = 1000
        NAME = "undefined"

        @staticmethod
        def get_filters():
            raise NotImplementedError("NI")

        @staticmethod
        def requires_github_api():
            raise NotImplementedError("NI")

    def transformation(priority):
        """A decorator to assign a priority to a transformation method."""
        def decorator(func):
            func._priority = priority
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return decorator

    def __init__(self, args):
        self.G = git.Repo(args.git)
        self.R = self._redmine_connect()
        self.limit = args.limit
        self.session = requests.Session()
        self.issue_id = args.issue
        self.revision_range = args.revision_range
        self.pull_request_id = args.pull_request
        self.merge_commit = args.merge_commit

        self.remote_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}"
        self.upkeep_failures = 0
        self.issues_inspected = 0
        self.issues_modified = 0
        self.modifications_made = {} # Dictionary to store what transformations were applied

        self.project_id = None
        try:
            log.info("Fetching 'Ceph' project ID from Redmine.")
            project = self.R.project.get("Ceph")
            self.project_id = project['id']
            log.info(f"Found 'Ceph' project with ID: {self.project_id}")
        except redminelib.exceptions.ResourceAttrError:
            log.error("Project 'Ceph' not found in Redmine. Cannot filter issues by project.")
            sys.exit(1)

        # Discover transformation methods based on prefix
        self.transform_methods = []
        for name in dir(self):
            if name.startswith('_transform_') and callable(getattr(self, name)):
                self.transform_methods.append(getattr(self, name))
        log.debug(f"Discovered transformation methods: {[m.__name__ for m in self.transform_methods]}")

        # Sort transformations for consistent order if needed, e.g., by name
        self.transform_methods.sort(key=lambda f: f._priority, reverse=True)
        log.debug(f"Sorted transformation methods: {[m.__name__ for m in self.transform_methods]}")

        # Discover filters based on prefix
        self.filters = []
        for name, v in RedmineUpkeep.__dict__.items():
            if inspect.isclass(v) and issubclass(v, self.Filter) and v != self.Filter:
                log.debug("discovered filter %s", v.NAME)
                self.filters.append(v)
        random.shuffle(self.filters) # to shuffle equivalent PRIORITY
        self.filters.sort(key = lambda filter: filter.PRIORITY, reverse=True)
        log.debug(f"Discovered filters: {[f.__name__ for f in self.filters]}")

        self._fetch_heads()

    def _fetch_heads(self):
        log.info(f"Fetching remote heads from {self.remote_url}.")
        self.remote_heads = []
        for line in self.G.git.ls_remote('--heads', self.remote_url).split('\n'):
            (sha, name) = line.split('\t')
            final_component = name.rsplit('/', 1)[-1]
            if not final_component.isalpha():
                log.debug(f"Head {name} is not alphabetic, skipping.")
                continue
            log.debug(f"Adding Head {name} with commit {sha}.")
            try:
                self.remote_heads.append(self.G.commit(sha))
            except ValueError as e:
                log.debug(f"Could not load commit {sha}, attempting to fetch.")
                self.G.git.fetch(self.remote_url, sha)
                try:
                    self.remote_heads.append(self.G.commit(sha))
                except ValueError as e:
                    log.error(f"Error: Could not fetch commit {sha}")
                    continue

    def _redmine_connect(self):
        log.info("Connecting to %s", REDMINE_ENDPOINT)
        R = redminelib.Redmine(REDMINE_ENDPOINT, key=REDMINE_API_KEY)
        log.info("Successfully connected to Redmine.")
        return R

    class FilterMergedBug1(Filter):
        """
        Filter issues with erroneous merge commits.
        """

        PRIORITY = 1100
        NAME = "MergedBug"

        @staticmethod
        def get_filters():
            filter_set = {
                f"cf_{REDMINE_CUSTOM_FIELD_ID_PULL_REQUEST_ID}": '>=0',
                f"cf_{REDMINE_CUSTOM_FIELD_ID_RELEASED_IN}": '~^',
            }
            yield {**filter_set, **{f"cf_{REDMINE_CUSTOM_FIELD_ID_TAGS}": "!*"}}
            yield {**filter_set, **{f"cf_{REDMINE_CUSTOM_FIELD_ID_TAGS}": "!~upkeep-bad-parentage"}}

        @staticmethod
        def requires_github_api():
            return True

    @transformation(10000)
    def _transform_clear_stale_merge_commit(self, issue_update):
        """
        Transformation: If the "Pull Request ID" was changed after the "Merge
        Commit SHA" was set, this transformation clears the merge commit and
        related "Fixed In" field, as they are now considered stale.
        """
        issue_update.logger.debug("Running _transform_clear_stale_merge_commit")
        try:
            issue_with_journals = self.R.issue.get(issue_update.issue.id, include=['journals'])
        except redminelib.exceptions.ResourceNotFoundError:
            issue_update.logger.warning("Could not fetch issue with journals. Skipping stale merge commit check.")
            return False

        last_pr_id_change = None
        last_merge_commit_set = None

        # Journals are ordered oldest to newest, so reverse to find the latest changes first.
        for journal in reversed(issue_with_journals.journals):
            if last_pr_id_change and last_merge_commit_set:
                break

            for detail in journal.details:
                if detail.get('property') == 'cf':
                    try:
                        field_id = int(detail.get('name'))
                        if field_id == REDMINE_CUSTOM_FIELD_ID_PULL_REQUEST_ID and not last_pr_id_change:
                            last_pr_id_change = journal.id
                            issue_update.logger.debug(f"last_pr_id_change = {last_pr_id_change}")
                        elif field_id == REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT and not last_merge_commit_set:
                            # We only care when the commit was set to a non-empty value.
                            if detail.get('new_value'):
                                last_merge_commit_set = journal.id
                                issue_update.logger.debug(f"last_merge_commit_set = {last_merge_commit_set}")
                    except (ValueError, TypeError):
                        continue # Ignore if 'name' is not a valid integer field ID

        if not last_pr_id_change or not last_merge_commit_set:
            issue_update.logger.debug("Did not find journal entries for both PR ID and Merge Commit changes. No action taken.")
            return False

        issue_update.logger.debug(f"Last PR ID change: {last_pr_id_change}, Last Merge Commit set: {last_merge_commit_set}")

        if last_pr_id_change > last_merge_commit_set:
            issue_update.logger.info("The 'Pull Request ID' was changed after the 'Merge Commit SHA' was set. Clearing the stale merge commit.")
            # Clear the merge commit field and also the 'Fixed In' field which depends on it.
            changed = False
            changed |= issue_update.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT, "")
            changed |= issue_update.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_FIXED_IN, "")
            changed |= issue_update.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_RELEASED_IN, "")
            return changed

        return False

    class FilterMerged(Filter):
        """
        Filter issues that are closed but no merge commit is set.
        """

        PRIORITY = 1000
        NAME = "Merged"

        @staticmethod
        def get_filters():
            statuses = [
                REDMINE_STATUS_ID_PENDING_BACKPORT,
                REDMINE_STATUS_ID_RESOLVED,
            ]
            for status in statuses:
                yield {
                    f"cf_{REDMINE_CUSTOM_FIELD_ID_PULL_REQUEST_ID}": '>=0',
                    f"cf_{REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT}": '!*',
                    "status_id": str(status),
                }

        @staticmethod
        def requires_github_api():
            return True

    def _find_merge_commit(self, issue_update, HEAD, BASE):
        """
        Find merge commit in revrange.
        """

        # N.B. At the time of writing, using "-1" to limit output breaks the command (returns nothing always).
        try:
            revrange = f"{HEAD.hexsha}^..{BASE.hexsha}"
            merge_commit = self.G.git.log(
                '--first-parent',
                '--merges',
                '--reverse',
                f"--ancestry-path={HEAD.hexsha}",
                '--pretty=%H',
                revrange
            ).splitlines()
            if len(merge_commit) == 0:
                issue_update.logger.debug(f"No commit found in first-parent lineage.")
            else:
                merge = self.G.commit(merge_commit[0])
                m = self._confirm_merge_commit(issue_update, merge, HEAD, BASE)
                if m:
                    return m.hexsha
        except git.exc.GitCommandError as e:
            issue_update.logger.error(f"Error: Could not find merge commit")

        # Try non-first parent lineage.
        try:
            revrange = f"{HEAD.hexsha}^..{BASE.hexsha}"
            merge_commit = self.G.git.log(
                '--merges',
                '--reverse',
                f"--ancestry-path={HEAD.hexsha}",
                '--pretty=%H',
                revrange
            ).splitlines()
            if len(merge_commit) == 0:
                issue_update.logger.debug(f"No commit found in first-parent lineage.")
            else:
                merge = self.G.commit(merge_commit[0])
                m = self._confirm_merge_commit(issue_update, merge, HEAD, BASE)
                if m:
                    return m.hexsha
        except git.exc.GitCommandError as e:
            issue_update.logger.error(f"Error: Could not find merge commit")

    def _confirm_merge_commit(self, issue_update, merge, HEAD, BASE):
        """
        Confirm merge commit is correct.
        """
        issue_update.logger.debug(f"Confirming merge commit {merge}")

        if len(merge.parents) <= 1:
            # not a merge commit
            return None

        second_parent = merge.parents[1]
        if second_parent == HEAD:
            issue_update.logger.debug(f"Found valid merge commit against {BASE}: {merge}")
            return merge
        else:
            issue_update.logger.warning(f"Merge commit second parent is not {HEAD}. Ignoring this merge.")
            return None

    def _get_merge_commit(self, issue_update):
        """
        Figure out the merge commit from the head reference of the PR.
        """
        pr_id = issue_update.get_pr_id()

        ref = f"refs/pull/{pr_id}/head"

        try:
            self.G.git.fetch(self.remote_url, ref)
            HEAD = self.G.commit('FETCH_HEAD')
            issue_update.logger.info(f"Pull Request head is {HEAD}.")
        except git.exc.GitCommandError as e:
            issue_update.logger.error(f"Error: Could not fetch reference '{ref}' from {self.remote_url}.")
            issue_update.logger.error(f"Git Error: {e}")
            return None

        for BASE in self.remote_heads:
            issue_update.logger.info(f"Examining remote branch HEAD {BASE}.")

            m = self._find_merge_commit(issue_update, HEAD, BASE)
            if m:
                return m

        # Fall back to API query
        pr = issue_update.fetch_pr()
        if not pr:
            issue_update.logger.info("No PR data found. Skipping merge check.")
            return None

        merged = pr.get('merged')
        if not merged:
            if pr.get('state') == "closed":
                raise PRClosedException(issue_update, pr_id)
            issue_update.logger.info(f"PR #{pr_id} is not merged. Skipping merge check.")
            return None

        # N.B. merge_commit_sha is sometimes wrong because of branch renames.

        base = pr.get('base')
        if not base:
            issue_update.logger.info(f"PR #{pr_id} is merged but has no base?")
            return None

        issue_update.logger.info(f"PR #{pr_id} base is {base['ref']}")

        try:
            BASE = self.G.commit(base['sha'])
        except git.exc.GitCommandError as e:
            issue_update.logger.debug(f"Fetching {base['ref']}")
            self.G.git.fetch(self.remote_url, base['ref'])
            BASE = self.G.commit('FETCH_HEAD')

        m = self._find_merge_commit(issue_update, HEAD, BASE)
        if m:
            return m

        issue_update.logger.info(f"Could not find a merge commit for PR #{pr_id}")
        return None


    @transformation(1000)
    def _transform_merged(self, issue_update):
        """
        Transformation: Checks if a PR associated with an issue has been merged
        and updates the merge commit and fixed_in fields in the payload.
        """
        issue_update.logger.debug("Running _transform_merged")

        commit = issue_update.get_custom_field(REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT)

        # Fixing bug: GitHub got confused by branch renames and gives the wrong
        # merge commit. This is detectable by a "Release In" setting like:
        # v15.2.0~1225^2. Note: sometimes this is self-inflicted because hotfix
        # branches can mess up the first-parent line of succession.
        released_in = issue_update.get_custom_field(REDMINE_CUSTOM_FIELD_ID_RELEASED_IN)
        if released_in and "^" in released_in:
            issue_update.logger.warning(f"Detected GitHub bug where past merge commit is wrong: {commit}")
            commit = None

        if not commit:
            issue_update.logger.info("Merge commit not set, will check PR status.")

            commit = self._get_merge_commit(issue_update)
            if commit:
                issue_update.logger.info(f"Merge commit is {commit}")
                issue_update.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT, commit)
            else:
                issue_update.logger.info(f"No merge commit is known")
                return False
        else:
            issue_update.logger.info(f"Merge commit {commit} is already set. Skipping PR fetch.")

        try:
            issue_update.logger.info(f"Running git describe for commit {commit}")
            ref = issue_update.git_repo.git.describe('--always', '--abbrev=10', commit)
            issue_update.logger.info(f"Git describe output: {ref}")
            changed = issue_update.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_FIXED_IN, ref)
            return changed
        except git.exc.GitCommandError as e:
            issue_update.logger.warning(f"Could not get git describe for commit {commit}: {e}")
        return False

    @transformation(10)
    def _transform_backport_resolved(self, issue_update):
        """
        Transformation: Changes backport trackers to "Resolved" if the associated PR is merged.
        """
        issue_update.logger.debug("Running _transform_backport_resolved")

        # Check if it's a backport tracker
        if issue_update.issue.tracker.id != REDMINE_TRACKER_ID_BACKPORT:
            issue_update.logger.info("Not a backport tracker. Skipping backport resolved check.")
            return False
        issue_update.logger.info("Issue is a backport tracker.")

        commit = issue_update.get_custom_field(REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT)
        if not commit:
           issue_update.logger.info(f"No merge commit set, skipping.")
           return False

        # If PR is merged and it's a backport tracker with 'Pending Backport' status, update to 'Resolved'
        if issue_update.issue.status.id != REDMINE_STATUS_ID_RESOLVED:
            issue_update.logger.info(f"Issue status is '{issue_update.issue.status.name}', which is not 'Resolved'.")
            issue_update.logger.info("Updating status to 'Resolved' because its PR is merged.")
            changed = issue_update.change_field('status_id', REDMINE_STATUS_ID_RESOLVED)
            return changed
        else:
            issue_update.logger.info("Issue is already in 'Resolved' status. No change needed.")
            return False

    class FilterReleased(Filter):
        """
        Filter for issues that are merged but not yet released.
        """

        PRIORITY = 10
        NAME = "Released"

        @staticmethod
        def get_filters():
            yield {
                f"cf_{REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT}": '*',
                f"cf_{REDMINE_CUSTOM_FIELD_ID_RELEASED_IN}": '!*',
                "status_id": "*",
            }

        @staticmethod
        def requires_github_api():
            return False

    @transformation(10)
    def _transform_released(self, issue_update):
        """
        Transformation: Checks if a merged issue has been released and updates
        the 'Released In' field in the payload.
        """
        issue_update.logger.debug("Running _transform_released")
        commit = issue_update.get_custom_field(REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT)
        if not commit:
            issue_update.logger.info("No merge commit set. Skipping released check.")
            return False
        issue_update.logger.info(f"Checking release status for merge commit: {commit}")

        released_in = issue_update.get_custom_field(REDMINE_CUSTOM_FIELD_ID_RELEASED_IN)
        issue_update.logger.info(f"'Released In' currently '{released_in}'")

        release = issue_update.get_released(commit)

        if release and "^" in release:
            issue_update.logger.warning(f"Detected parentage linkage issue (first parent chain broken) by hotfix: {release}")
            issue_update.add_tag('upkeep-bad-parentage')

        if release:
            issue_update.logger.info(f"Commit {commit} is part of release {release}.")
            changed = issue_update.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_RELEASED_IN, release)
            return changed
        elif released_in:
            issue_update.logger.error(f"'Released In' would be cleared (currently: '{released_in}')??")
        else:
            issue_update.logger.info(f"Commit {commit} not yet in a release. 'Released In' field will not be updated.")
        return False


    class FilterPendingBackport(Filter):
        """
        Filter for issues that are in 'Pending Backport' status.  The
        transformation will then check if they are non-backport trackers and if
        all their 'Copied to' backports are resolved.
        """

        PRIORITY = 10
        NAME = "Pending Backport"

        @staticmethod
        def get_filters():
            yield {
                f"cf_{REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT}": '*',
                "status_id": str(REDMINE_STATUS_ID_PENDING_BACKPORT),
            }

        @staticmethod
        def requires_github_api():
            return False


    @transformation(10)
    def _transform_resolve_main_issue_from_backports(self, issue_update):
        """
        Transformation: Resolves a main issue if all its "Copied to" backport
        issues are resolved and correctly tagged with the expected backport
        releases.
        """
        issue_update.logger.debug("Running _transform_resolve_main_issue_from_backports")

        if issue_update.issue.tracker.id == REDMINE_TRACKER_ID_BACKPORT:
            issue_update.logger.info("Is a backport tracker. Skipping this transformation.")
            return False

        if issue_update.issue.status.id != REDMINE_STATUS_ID_PENDING_BACKPORT:
            issue_update.logger.info(f"Not in 'Pending Backport' status ({issue_update.issue.status.name}). Skipping.")
            return False

        issue_update.logger.info("Issue is a main tracker in 'Pending Backport' status. Checking related backports.")

        expected_backport_releases_str = issue_update.get_custom_field(REDMINE_CUSTOM_FIELD_ID_BACKPORT)
        if expected_backport_releases_str:
            expected_backport_releases = set(
                rel.strip() for rel in expected_backport_releases_str.split(',') if rel.strip()
            )
            issue_update.logger.info(f"Expecting backports for releases: {expected_backport_releases}")
        else:
            expected_backport_releases = set()
            issue_update.logger.warning(f"No backport releases specified in custom field {REDMINE_CUSTOM_FIELD_ID_BACKPORT}.")

        copied_to_backports_ids = []
        try:
            # Fetch the issue again with 'include=relations' to ensure relations are loaded
            issue_update.logger.debug("Fetching issue relations to find 'copied_to' links.")
            issue_with_relations = self.R.issue.get(issue_update.issue.id, include=['relations'])

            for relation in issue_with_relations.relations:
                if relation.relation_type == 'copied_to':
                    copied_to_backports_ids.append(relation.issue_to_id)
            issue_update.logger.info(f"Found 'Copied to' issue IDs: {copied_to_backports_ids}")
        except redminelib.exceptions.ResourceAttrError as e:
            issue_update.logger.warning(f"Could not fetch relations for issue: {e}. Skipping backport status check.")
            return False

        if not copied_to_backports_ids and not expected_backport_releases:
            # If no backports are expected and no 'Copied to' issues exist,
            # then the main issue can be resolved.
            issue_update.logger.info("No backports expected and no 'Copied to' issues found. Updating main issue status to 'Resolved'.")
            changed = issue_update.change_field('status_id', REDMINE_STATUS_ID_RESOLVED)
            return changed
        elif not copied_to_backports_ids and expected_backport_releases:
            # If backports are expected but no 'Copied to' issues exist,
            # the main issue cannot be resolved.
            issue_update.logger.info(f"Backports expected ({', '.join(expected_backport_releases)}) but no 'Copied to' issues found. Main issue cannot be resolved.")
            return False

        resolved_and_matched_backports = set()
        all_backports_resolved_and_matched = True

        for backport_id in copied_to_backports_ids:
            try:
                issue_update.logger.info(f"Checking status of backport issue #{backport_id}")
                backport_issue = self.R.issue.get(backport_id)
                
                # Ensure the related issue is actually a backport tracker
                if backport_issue.tracker.id != REDMINE_TRACKER_ID_BACKPORT:
                    issue_update.logger.warning(f"Related issue #{backport_id} is 'Copied to' but not a backport tracker. Ignoring it for resolution check.")
                    continue

                # Check backport issue's release field
                cf_backport_release = backport_issue.custom_fields.get(REDMINE_CUSTOM_FIELD_ID_RELEASE)
                if not cf_backport_release:
                    issue_update.logger.info(f"Backport issue #{backport_id} has no release specified in custom field {REDMINE_CUSTOM_FIELD_ID_RELEASE}. Cannot resolve main issue yet.")
                    all_backports_resolved_and_matched = False
                    break

                backport_release = cf_backport_release.value
                issue_update.logger.debug(f"Backport issue #{backport_id} is for release '{backport_release}'.")
                if backport_release not in expected_backport_releases:
                    issue_update.logger.info(f"Backport issue #{backport_id} has release '{backport_release}' which is not in expected backports ({', '.join(expected_backport_releases)}). Main issue cannot be resolved yet.")
                    all_backports_resolved_and_matched = False
                    break

                if backport_issue.status.id == REDMINE_STATUS_ID_RESOLVED:
                    issue_update.logger.info(f"Backport issue #{backport_id} is resolved and matches expected release '{backport_release}'.")
                    resolved_and_matched_backports.add(backport_release)
                elif backport_issue.status.id == REDMINE_STATUS_ID_REJECTED:
                    issue_update.logger.info(f"Backport issue #{backport_id} is rejected and matches expected release '{backport_release}'.")
                    resolved_and_matched_backports.add(backport_release)
                else:
                    issue_update.logger.info(f"Backport issue #{backport_id} is not resolved or rejected (status: {backport_issue.status.name}). Main issue cannot be resolved yet.")
                    all_backports_resolved_and_matched = False
                    break
            except redminelib.exceptions.ResourceNotFoundError:
                issue_update.logger.warning(f"Related backport issue #{backport_id} not found. Cannot confirm all backports resolved.")
                all_backports_resolved_and_matched = False # Treat as not resolved if we can't find it
                break
            except redminelib.exceptions.ResourceAttrError:
                issue_update.logger.warning(f"Related backport issue #{backport_id} not accessible. Cannot confirm all backports resolved.")
                all_backports_resolved_and_matched = False # Treat as not resolved if we can't find it
                break

        # Final check: all backports found, resolved, correctly tagged, and all expected backports are covered
        if all_backports_resolved_and_matched and expected_backport_releases == resolved_and_matched_backports:
            issue_update.logger.info(f"All expected backport releases ({', '.join(expected_backport_releases)}) have corresponding resolved and correctly tagged 'Copied to' issues. Updating main issue status to 'Resolved'.")
            issue_update.change_field('status_id', REDMINE_STATUS_ID_RESOLVED)
            return True
        else:
            issue_update.logger.info("Not all expected backports are resolved and/or correctly tagged. Main issue status remains 'Pending Backport'.")
            issue_update.logger.info(f"Expected backports: {expected_backport_releases}")
            issue_update.logger.info(f"Resolved and matched backports found: {resolved_and_matched_backports}")
        return False


    class FilterUnresolvedMerged(Filter):
        """
        Filters for issues that have a merge commit set but are not yet in
        'Pending Backport' or 'Resolved' status.
        """

        PRIORITY = 100
        NAME = "Unresolved Merge"

        @staticmethod
        def get_filters():
            filters = {}
            filters[f"cf_{REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT}"] = '*'
            statuses = [
                REDMINE_STATUS_ID_NEW,
                REDMINE_STATUS_ID_INPROGRESS,
                REDMINE_STATUS_ID_TRIAGED,
                REDMINE_STATUS_ID_NEEDINFO,
                REDMINE_STATUS_ID_FIX_UNDER_REVIEW,
            ]
            for status in statuses:
                filters["status_id"] = str(status)
                yield filters

        @staticmethod
        def requires_github_api():
            return False

    @transformation(100)
    def _transform_set_status_on_merge(self, issue_update):
        """
        Transformation: Updates the status of an issue after its associated PR is merged.
        If the 'Backports' field contains entries, sets status to 'Pending Backport'.
        If 'Backports' is empty, sets status to 'Resolved'.
        """
        issue_update.logger.debug("Running _transform_set_status_on_merge")

        current_status_id = issue_update.issue.status.id
        merge_commit = issue_update.get_custom_field(REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT)
        if not merge_commit:
            issue_update.logger.info("No merge commit found. Skipping status update.")
            return False

        # Only proceed if the issue is not already in a final or pending backport state
        if issue_update.issue.status.is_closed or issue_update.issue.status.id == REDMINE_STATUS_ID_PENDING_BACKPORT:
            issue_update.logger.info(f"Issue is already closed or 'Pending Backport'. Skipping status update on merge.")
            return False

        issue_update.logger.info(f"Issue has a merge commit ({merge_commit}) and current status is '{issue_update.issue.status.name}'.")

        backports_field_value = issue_update.get_custom_field(REDMINE_CUSTOM_FIELD_ID_BACKPORT)
        backports_list = [bp.strip() for bp in (backports_field_value or "").split(',') if bp.strip()]

        if backports_list:
            # If 'Backports' field has entries, move to PENDING_BACKPORT
            if current_status_id != REDMINE_STATUS_ID_PENDING_BACKPORT:
                issue_update.logger.info(f"Backports defined: {backports_list}. Setting status to 'Pending Backport'.")
                return issue_update.change_field('status_id', REDMINE_STATUS_ID_PENDING_BACKPORT)
            else:
                issue_update.logger.info("Status is already 'Pending Backport'. No change needed.")
                return False
        else:
            # If 'Backports' field is empty, move to RESOLVED
            if current_status_id != REDMINE_STATUS_ID_RESOLVED:
                issue_update.logger.info("No backports defined. Setting status to 'Resolved'.")

                if self.pull_request_id:
                    comment_body = f"""
                        This is an automated message by src/script/redmine-upkeep.py.

                        I have resolved the following tracker ticket due to the merge of this PR:

                        * {issue_update.issue.url}

                        No backports are pending for the ticket. If this is incorrect, please update the tracker
                        ticket and reset to `Pending Backport` state.
                    """
                    if GITHUB_ACTIONS:
                        comment_body += f"""

                        Update Log: {GITHUB_ACTION_LOG}
                        """

                    comment_body = textwrap.dedent(comment_body)
                    issue_update.logger.debug(f"Leaving comment:\n{comment_body}")

                    post_github_comment(self.session, self.pull_request_id, comment_body)

                return issue_update.change_field('status_id', REDMINE_STATUS_ID_RESOLVED)
            else:
                issue_update.logger.info("Status is already 'Resolved'. No change needed.")
                return False


    def _process_issue_transformations(self, issue):
        """
        Applies all discovered transformation methods to a single Redmine issue
        and sends a single update API call if changes are made.
        """
        self.issues_inspected += 1
        issue_update = IssueUpdate(issue, self.session, self.G)
        issue_update.logger.debug("Beginning transformation processing.")

        if IS_GITHUB_ACTION:
            log_stream.flush()
            print(f"::group::Processing Issue #{issue.id}: {issue.subject}", file=sys.stderr, flush=True) # Start GitHub Actions group
            issue_update.logger.info(f"Issue URL: {issue.url}")
        else:
            issue_update.logger.info(f"Processing issue: {issue.url} '{issue.subject}'")

        try:
            applied_transformations = []
            for transform_method in self.transform_methods:
                # Each transformation method modifies the same issue_update object
                transform_name = transform_method.__name__.removeprefix("_transform_")
                issue_update.logger.debug(f"Calling transformation: {transform_name}")
                issue_update.set_transform(transform_name)
                if transform_method(issue_update):
                    issue_update.logger.info(f"Transformation {transform_method.__name__} resulted in a change.")
                    applied_transformations.append(transform_method.__name__)

            issue_update.set_transform(None)
            if issue_update.has_changes:
                issue_update.logger.info("Changes detected. Sending update to Redmine...")
                try:
                    # We cannot put top-level changes in the PUT request against
                    # the redmine API via redminelib. So we send it manually.
                    payload = issue_update.get_update_payload()
                    issue_update.logger.debug("PUT payload:\n%s", json.dumps(payload, indent=4))
                    headers = {
                        'Content-Type': 'application/json',
                        'X-Redmine-API-Key': REDMINE_API_KEY,
                    }
                    endpoint = f"{REDMINE_ENDPOINT}/issues/{issue.id}.json"
                    response = requests.put(endpoint, headers=headers, data=json.dumps(payload))
                    response.raise_for_status()
                    issue_update.logger.info("Successfully updated Redmine issue.")
                    self.issues_modified += 1
                    for t_name in applied_transformations:
                        self.modifications_made.setdefault(t_name, 0)
                        self.modifications_made[t_name] += 1
                    return True
                except requests.exceptions.HTTPError as e:
                    issue_update.logger.error("API PUT failure during upkeep.", exc_info=True)
                    raise RedmineUpdateException(issue_update, exception=e, traceback=traceback.format_exc())
            else:
                issue_update.logger.info("No changes detected after all transformations. No Redmine update sent.")
                return False
        except UpkeepException as e:
            self._handle_upkeep_failure(issue_update, e)
            return False
        finally:
            issue_update.set_transform(None)
            if IS_GITHUB_ACTION:
                log_stream.flush()
                print(f"::endgroup::", file=sys.stderr, flush=False) # End GitHub Actions group

    def _handle_upkeep_failure(self, issue_update, error):
        """
        Adds a tag and an unsilenced comment to the issue when upkeep fails.
        """

        self.upkeep_failures += 1
        if self.upkeep_failures > self.MAX_UPKEEP_FAILURES:
            raise RuntimeError("too many upkeep failures: assuming systemic bug and quitting!")

        issue_id = issue_update.issue.id
        issue_update.logger.error(f"Upkeep failed for issue #{issue_id}. Attempting to add 'upkeep-failed' tag and comment.")

        # Prepare payload for failure update
        failure_payload = {
            'issue': {},
            'suppress_mail': "0", # Do not suppress mail for failure notification
        }

        # Add comment
        comment = f"""
h1. Redmine Upkeep failure

The "redmine-upkeep.py script":https://github.com/ceph/ceph/blob/main/src/script/redmine-upkeep.py failed to update this issue. I have added the tag "upkeep-failed" to avoid looking at this issue again.

**Please manually fix the issue and remove "upkeep-failed" tag to allow future upkeep operations.**

h2. Transformation

The script was in the *{issue_update.transform}* transformation.

h2. Update Log

{GITHUB_ACTION_LOG}

h2. Error

{error.comment()}
"""

        if issue_update.has_changes:
            comment += f"""
h2. Update Payload

<pre>
{json.dumps(issue_update.get_update_payload(suppress_mail=True), indent=4)}
</pre>

"""
        comment = comment.strip()
        issue_update.logger.debug("Created update failure comment:\n%s", comment)
        failure_payload['issue']['notes'] = comment

        # Get existing tags or initialize if none
        current_tags_str = issue_update.get_raw_custom_field(REDMINE_CUSTOM_FIELD_ID_TAGS)
        current_tags = []
        if current_tags_str:
            current_tags = [tag.strip() for tag in current_tags_str.split(',') if tag.strip()]

        new_tag = "upkeep-failed"
        if new_tag in current_tags:
            issue_update.logger.warning(f"'upkeep-failed' tag is already present")
            return
        else:
            current_tags.append(new_tag)
            issue_update.logger.info(f"Adding '{new_tag}' tag.")

        # Update custom field for tags in the failure payload
        custom_fields_payload = failure_payload['issue'].setdefault('custom_fields', [])
        custom_fields_payload.append(
            {'id': REDMINE_CUSTOM_FIELD_ID_TAGS, 'value': ", ".join(current_tags)}
        )

        try:
            issue_update.logger.info("Sending failure notification update to Redmine...")
            headers = {
                'Content-Type': 'application/json',
                'X-Redmine-API-Key': REDMINE_API_KEY,
            }
            endpoint = f"{REDMINE_ENDPOINT}/issues/{issue_id}.json"
            response = requests.put(endpoint, headers=headers, data=json.dumps(failure_payload))
            response.raise_for_status()
            issue_update.logger.info(f"Successfully added 'upkeep-failed' tag and comment to Redmine issue.")
        except requests.exceptions.HTTPError as err:
            issue_update.logger.fatal(f"Could not update Redmine issue with failure tag/comment: {err} - Response: {response.text}")
            sys.exit(1)

    def filter_and_process_issues(self):
        """
        Fetches issues based on filters or revision range/specific issue ID and
        processes each one using all registered transformations.
        """
        log.info("Starting to filter and process issues.")
        if self.issue_id is not None:
            log.info(f"Processing in single-issue mode for issue #{self.issue_id}.")
            try:
                issue = self.R.issue.get(self.issue_id)
                self._process_issue_transformations(issue)
            except redminelib.exceptions.ResourceNotFoundError:
                log.error(f"Issue #{self.issue_id} not found in Redmine.")
                sys.exit(1)
            except redminelib.exceptions.ResourceAttrError:
                log.error(f"Issue #{self.issue_id} not found in Redmine.")
                sys.exit(1)
        elif self.revision_range is not None:
            log.info(f"Processing in revision-range mode for range: {self.revision_range}.")
            self._execute_revision_range()
        elif self.pull_request_id is not None:
            log.info(f"Processing in pull-request mode for PR #{self.pull_request_id}.")
            self._execute_pull_request()
        else:
            log.info(f"Processing in filter-based mode with a limit of {self.limit} issues.")
            self._execute_filters()

    def _execute_pull_request(self):
        """
        Handles the --pull-request logic.
        1. Finds Redmine issues linked to the PR and runs transforms on them.
        2. If none, inspects the local merge commit for "Fixes:" tags.
        3. If tags are found, comments on the GH PR to ask the author to link the ticket.
        """
        pr_id = self.pull_request_id
        merge_commit_sha = self.merge_commit
        log.info(f"Querying Redmine for issues linked to PR #{pr_id} and merge commit {merge_commit_sha}")

        filters = {
            "project_id": self.project_id,
            "status_id": "*",
            f"cf_{REDMINE_CUSTOM_FIELD_ID_PULL_REQUEST_ID}": pr_id,
        }
        issues = self.R.issue.filter(**filters)

        processed_issue_ids = set()
        if len(issues) > 0:
            log.info(f"Found {len(issues)} linked issue(s). Applying transformations.")
            for issue in issues:
                self._process_issue_transformations(issue)
                processed_issue_ids.add(issue.id)
            # Still, check commit logs.
        else:
            log.warning(f"No Redmine issues found linked to PR #{pr_id}. Inspecting local merge commit {merge_commit_sha} for 'Fixes:' tags.")

        found_tracker_ids = set()
        try:
            revrange = f"{merge_commit_sha}^..{merge_commit_sha}"
            log.info(f"Iterating commits {revrange}")
            for commit in self.G.iter_commits(revrange):
                log.info(f"Inspecting commit {commit.hexsha}")

                fixes_regex = re.compile(r"Fixes: https://tracker.ceph.com/issues/(\d+)", re.MULTILINE)
                commit_fixes = set(fixes_regex.findall(commit.message))
                for tracker_id in commit_fixes:
                    log.info(f"Commit {commit.hexsha} claims to fix https://tracker.ceph.com/issues/{tracker_id}")
                    found_tracker_ids.add(int(tracker_id))
        except git.exc.GitCommandError as e:
            log.error(f"Git command failed for commit SHA '{merge_commit_sha}': {e}. Ensure the commit exists in the local repository.")
            return

        # Are the found_tracker_ids (including empty set) a proper subset of processed_issue_ids?
        log.debug(f"found_tracker_ids = {found_tracker_ids}")
        log.debug(f"processed_issue_ids = {processed_issue_ids}")
        if found_tracker_ids <= processed_issue_ids:
            log.info("All commits reference trackers already processed or no tracker referenced to be fixed.")
            return

        log.info(f"Found 'Fixes:' tags for tracker(s) #{', '.join([str(x) for x in found_tracker_ids])} in commits.")

        tracker_links = "\n".join([f"* https://tracker.ceph.com/issues/{tid}" for tid in found_tracker_ids])
        comment_body = f"""
            This is an automated message by src/script/redmine-upkeep.py.

            I found one or more `Fixes:` tags in the commit messages in

            `git log {revrange}`

            The referenced tickets are:

            {tracker_links}

            Those tickets do not reference this merged Pull Request. If this Pull Request merge resolves any of those tickets, please update the "Pull Request ID" field on each ticket. A future run of this script will appropriately update them.
        """
        if GITHUB_ACTIONS:
            comment_body += f"""

            Update Log: {GITHUB_ACTION_LOG}
            """
        comment_body = textwrap.dedent(comment_body)
        log.debug(f"Leaving comment:\n{comment_body}")

        post_github_comment(self.session, pr_id, comment_body)

    def _execute_revision_range(self):
        log.info(f"Processing issues based on revision range: {self.revision_range}")
        try:
            # Get first-parent merge commits in the revision range
            log.info("Querying git for merge commits in range.")
            merge_commits = self.G.git.log(
                '--first-parent',
                '--merges',
                '--pretty=%H',
                self.revision_range
            ).splitlines()
            log.info(f"Found {len(merge_commits)} merge commits in range.")
            log.debug(f"Found merge commits: {merge_commits}")

            for commit in merge_commits:
                log.info(f"Querying Redmine for issues with merge commit: {commit}")
                try:
                    # Query Redmine for issues with the specific merge commit
                    filters = {
                        "project_id": self.project_id,
                        "status_id": "*",
                        f"cf_{REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT}": commit,
                    }
                    issues = self.R.issue.filter(**filters)
                    issue_count = len(issues)
                    if issue_count > 0:
                        log.info(f"Found {issue_count} issue(s) for commit {commit}.")
                        for issue in issues:
                            self._process_issue_transformations(issue)
                    else:
                        log.info(f"No issues found for commit {commit}.")
                except redminelib.exceptions.ResourceAttrError as e:
                    log.error(f"Redmine API error for merge commit {commit}: {e}")
                    raise
        except git.exc.GitCommandError as e:
            log.error(f"Git command error for revision range '{self.revision_range}': {e}")
            raise

    def _execute_filters(self):
        limit = self.limit

        now = datetime.now(timezone.utc)
        one_week_ago = now - timedelta(days=7)
        cutoff_date = one_week_ago.isoformat(timespec='seconds')

        # Combine filters to capture issues that might need either transformation
        # This reduces Redmine API calls for filtering
        common_filters = {
            "project_id": self.project_id,
            "sort": f'cf_{REDMINE_CUSTOM_FIELD_ID_UPKEEP_TIMESTAMP}',
            "status_id": "*",
        }
        upkeep_failed_filters = [
            {f"cf_{REDMINE_CUSTOM_FIELD_ID_TAGS}": "!*",},
            {f"cf_{REDMINE_CUSTOM_FIELD_ID_TAGS}": "!~upkeep-failed",}
        ]
        #f"cf_{REDMINE_CUSTOM_FIELD_ID_UPKEEP_TIMESTAMP}": f"<={cutoff_date}", # Not updated recently

        log.info("Beginning to loop through filters.")
        for f in self.filters:
            if limit <= 0:
                log.info("Issue processing limit reached. Stopping filter execution.")
                break
            for filter_set in f.get_filters():
                log.debug(f"Generated filter set: {filter_set}")
                for upkeep_failed_filter in upkeep_failed_filters:
                    issue_filter = {**common_filters, **upkeep_failed_filter, **filter_set}
                    issue_filter['limit'] = limit
                    needs_github_api = f.requires_github_api()
                    try:
                        log.info(f"Running filter {f.NAME} with criteria: {issue_filter}")
                        issues = self.R.issue.filter(**issue_filter)
                        issue_count = len(issues)
                        log.info(f"Filter {f.NAME} returned {issue_count} issue(s).")
                        for issue in issues:
                            if needs_github_api and self.GITHUB_RATE_LIMITED:
                                log.warning(f"Stopping filter {f.NAME} due to Github rate limits.")
                                break
                            limit = limit - 1
                            self._process_issue_transformations(issue)
                            if limit <= 0:
                                break
                    except redminelib.exceptions.ResourceAttrError as e:
                        log.warning(f"Redmine API error with filter {issue_filter}: {e}")
                    if limit <= 0:
                        break

def main():
    parser = argparse.ArgumentParser(description="Ceph redmine upkeep tool")
    parser.add_argument('--debug', dest='debug', action='store_true', help='turn debugging on')
    parser.add_argument('--github-action', default=GITHUB_ACTIONS, dest='gha', action='store_true', help='github action output')
    parser.add_argument('--limit', dest='limit', action='store', type=int, default=200, help='limit processed issues')
    parser.add_argument('--git-dir', dest='git', action='store', default=".", help='git directory')

    # Mutually exclusive group for different modes of operation
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--issue', dest='issue', action='store', help='Single issue ID to check.')
    group.add_argument('--revision-range', dest='revision_range', action='store',
                       help='Git revision range to find merge commits and process related issues.')
    group.add_argument('--pull-request', dest='pull_request', type=int, action='store',
                       help='Pull Request ID to lookup (requires --merge-commit).')

    parser.add_argument('--merge-commit', dest='merge_commit', action='store',
                       help='Merge commit SHA for the PR (requires --pull-request).')

    args = parser.parse_args(sys.argv[1:])

    # Ensure --pull-request and --merge-commit are used together
    if args.pull_request and not args.merge_commit:
        parser.error("--pull-request and --merge-commit must be used together.")
        sys.exit(1)

    log.info("Redmine Upkeep Script starting.")

    global IS_GITHUB_ACTION
    if args.gha:
        IS_GITHUB_ACTION = True
        log.info("GitHub Actions output format enabled.")

    if args.debug:
        log.setLevel(logging.DEBUG)
        log.info("Debug logging enabled.")
        git_logger = logging.getLogger('git.cmd')
        git_logger.setLevel(logging.DEBUG)
        git_logger.addHandler(logging.StreamHandler(sys.stderr))
        requests_logger = logging.getLogger("requests.packages.urllib3")
        requests_logger.setLevel(logging.DEBUG)
        requests_logger.propagate = True

    log.debug(f"Parsed arguments: {args}")

    if not REDMINE_API_KEY:
        log.fatal("REDMINE_API_KEY not found! Please set REDMINE_API_KEY environment variable or ~/.redmine_key.")
        sys.exit(1)

    if GITHUB_TOKEN is None:
        log.fatal("GITHUB_TOKEN not found! Please set GITHUB_TOKEN environment variable or ~/.github_token.")
        sys.exit(1)

    if IS_GITHUB_ACTION and GITHUB_REPOSITORY != "ceph/ceph":
        log.fatal("refusing to run ceph/ceph.git github action for repository {GITHUB_REPOSITORY}")
        sys.exit(0)

    RU = None
    try:
        RU = RedmineUpkeep(args)
        RU.filter_and_process_issues() # No arguments needed here anymore
    except Exception as e:
        log.fatal(f"An unhandled error occurred during Redmine upkeep: {e}", exc_info=True)
        if IS_GITHUB_ACTION:
             print(f"::error::An unhandled error occurred: {e}", file=sys.stderr)
        sys.exit(1)

    log.info("Redmine Upkeep Script finished.")
    if RU:
        log.info(f"Summary: Issues Inspected: {RU.issues_inspected}, Issues Modified: {RU.issues_modified}, Issues Failed: {RU.upkeep_failures}")
        if RU.issues_modified > 0:
            log.info(f"Modifications by Transformation: {RU.modifications_made}")
        if RedmineUpkeep.GITHUB_RATE_LIMITED:
            log.warning("GitHub API rate limit was encountered during execution.")

    # Generate GitHub Actions Job Summary
    if IS_GITHUB_ACTION and RU:
        summary_file = os.getenv('GITHUB_STEP_SUMMARY')
        if summary_file:
            log.info(f"Writing summary to {summary_file}")
            with open(summary_file, 'a') as f:
                f.write(f"### Redmine Upkeep Summary\n")
                f.write(f"- Issues Inspected: {RU.issues_inspected}\n")
                f.write(f"- Issues Modified: {RU.issues_modified}\n")
                if RU.upkeep_failures > 0:
                    f.write(f"- Issues upkeep failures: {RU.upkeep_failures}\n")
                if RedmineUpkeep.GITHUB_RATE_LIMITED:
                    f.write(f"- **Warning:** GitHub API rate limit was encountered. Some GitHub-related transformations might have been skipped.\n")
                if RU.issues_modified > 0:
                    f.write(f"#### Modifications by Transformation:\n")
                    for transform, count in RU.modifications_made.items():
                        f.write(f"- `{transform}`: {count} issues\n")
                f.write(f"\n")

    sys.exit(0)

if __name__ == "__main__":
    main()
