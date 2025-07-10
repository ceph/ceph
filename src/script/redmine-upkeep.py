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
import itertools
import json
import logging
import os
import random
import re
import signal
import sys
import textwrap

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

GITHUB_USER = os.getenv("GITHUB_USER", os.getenv("GITHUB_USER", getuser()))
GITHUB_ORG = "ceph"
GITHUB_REPO = "ceph"
GITHUB_API_ENDPOINT = f"https://api.github.com/repos/{GITHUB_ORG}/{GITHUB_REPO}"

REDMINE_CUSTOM_FIELD_ID_BACKPORT = 2
REDMINE_CUSTOM_FIELD_ID_RELEASE = 16
REDMINE_CUSTOM_FIELD_ID_PULL_REQUEST_ID = 21
REDMINE_CUSTOM_FIELD_ID_TAGS = 31
REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT = 33
REDMINE_CUSTOM_FIELD_ID_FIXED_IN = 34
REDMINE_CUSTOM_FIELD_ID_RELEASED_IN = 35
REDMINE_CUSTOM_FIELD_ID_UPKEEP_TIMESTAMP = 37

REDMINE_STATUS_ID_RESOLVED = 3
REDMINE_STATUS_ID_REJECTED = 6
REDMINE_STATUS_ID_FIX_UNDER_REVIEW = 13
REDMINE_STATUS_ID_PENDING_BACKPORT = 14

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

def gitauth():
    return (GITHUB_USER, GITHUB_TOKEN)

def post_github_comment(session, pr_id, body):
    """Helper to post a comment to a GitHub PR."""
    if RedmineUpkeep.GITHUB_RATE_LIMITED:
        log.warning("GitHub API rate limit hit previously. Skipping posting comment.")
        return False

    log.info(f"Posting a comment to GitHub PR #{pr_id}.")
    endpoint = f"{GITHUB_API_ENDPOINT}/issues/{pr_id}/comments"
    payload = {'body': body}
    try:
        response = session.post(endpoint, auth=gitauth(), json=payload)
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

class IssueUpdate:
    def __init__(self, issue, github_session, git_repo):
        self.issue = issue
        self.update_payload = {}
        self.github_session = github_session
        self.git_repo = git_repo
        self._pr_cache = {}
        self.has_changes = False # New flag to track if changes are made
        logger_extra = {
          'issue_id': issue.id,
          'current_transform': None,
        }
        self.logger = IssueLoggerAdapter(logging.getLogger(__name__), extra=logger_extra)

    def set_transform(self, transform):
        self.logger.extra['current_transform'] = transform

    def get_custom_field(self, field_id):
        """ Get the custom field, first from update_payload otherwise issue """
        custom_fields = self.update_payload.setdefault("custom_fields", [])
        for field in custom_fields:
            if field.get('id') == field_id:
                return field['value']
        cf = self.issue.custom_fields.get(field_id)
        try:
            return cf.value if cf else None
        except redminelib.exceptions.ResourceAttrError:
            return None

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

    def fetch_pr(self):
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
            pr_id = int(pr_id)
        except (ValueError, TypeError): # Handle None or non-integer values
            self.logger.warning(f"Invalid or missing PR ID '{pr_id}'. Cannot fetch PR.")
            return None

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
            response = self.github_session.get(endpoint, auth=gitauth(), params=params)
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

    def __init__(self, args):
        self.G = git.Repo(args.git)
        self.R = self._redmine_connect()
        self.limit = args.limit
        self.session = requests.Session()
        self.issue_id = args.issue
        self.revision_range = args.revision_range
        self.pull_request_id = args.pull_request
        self.merge_commit = args.merge_commit

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
        self.transform_methods.sort(key=lambda x: x.__name__)
        log.debug(f"Sorted transformation methods: {[m.__name__ for m in self.transform_methods]}")

        # Discover filter methods based on prefix
        self.filter_methods = []
        for name in dir(self):
            if name.startswith('_filter_') and callable(getattr(self, name)):
                self.filter_methods.append(getattr(self, name))
        log.debug(f"Discovered filter methods: {[f.__name__ for f in self.filter_methods]}")

        random.shuffle(self.filter_methods)
        log.debug(f"Shuffled filter methods for processing order: {[f.__name__ for f in self.filter_methods]}")

    def _redmine_connect(self):
        log.info("Connecting to %s", REDMINE_ENDPOINT)
        R = redminelib.Redmine(REDMINE_ENDPOINT, key=REDMINE_API_KEY)
        log.info("Successfully connected to Redmine.")
        return R

    # Transformations:

    def _filter_merged(self, filters):
        log.debug("Applying _filter_merged criteria.")
        filters[f"cf_{REDMINE_CUSTOM_FIELD_ID_PULL_REQUEST_ID}"] = '>=0'
        filters[f"cf_{REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT}"] = '!*'
        filters["status_id"] = [
            REDMINE_STATUS_ID_PENDING_BACKPORT,
            REDMINE_STATUS_ID_RESOLVED
        ]
        return True # needs github API

    def _transform_merged(self, issue_update):
        """
        Transformation: Checks if a PR associated with an issue has been merged
        and updates the merge commit and fixed_in fields in the payload.
        """
        issue_update.logger.debug("Running _transform_merged")

        commit = issue_update.get_custom_field(REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT)
        if not commit:
            issue_update.logger.info("Merge commit not set, will check PR status on GitHub.")
            pr = issue_update.fetch_pr()
            if not pr:
                issue_update.logger.info("No PR data found. Skipping merge check.")
                return False

            merged = pr.get('merged')
            if not merged:
                issue_update.logger.info(f"PR #{pr['number']} is not merged. Skipping merge check.")
                return False

            commit = pr.get('merge_commit_sha')
            if not commit:
                issue_update.logger.info(f"PR #{pr['number']} is merged but has no merge commit SHA. Skipping merge check.")
                return False

            issue_update.logger.info(f"PR #{pr['number']} merged with commit {commit}")
            issue_update.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT, commit)
        else:
            issue_update.logger.info(f"Merge commit {commit} is already set. Skipping PR fetch.")

        try:
            issue_update.logger.info(f"Running git describe for commit {commit}")
            ref = issue_update.git_repo.git.describe('--always', commit)
            issue_update.logger.info(f"Git describe output: {ref}")
            changed = issue_update.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_FIXED_IN, ref)
            return changed
        except git.exc.GitCommandError as e:
            issue_update.logger.warning(f"Could not get git describe for commit {commit}: {e}")
        return False

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
            issue_update.logger.info("Merge commit not set for backport tracker. Will check PR status.")
            # Check if the current status is 'Pending Backport'
            if issue_update.issue.status.id == REDMINE_STATUS_ID_RESOLVED:
                issue_update.logger.info("Status is already 'Resolved'. Skipping backport resolved check.")
                return False

            pr = issue_update.fetch_pr()
            if not pr:
                issue_update.logger.info("No PR data found. Skipping backport resolved check.")
                return False

            merged = pr.get('merged')
            if not merged:
                issue_update.logger.info(f"PR #{pr['number']} not merged. Skipping backport resolved check.")
                return False
            issue_update.logger.info(f"PR #{pr.get('number')} for backport tracker is merged.")

        # If PR is merged and it's a backport tracker with 'Pending Backport' status, update to 'Resolved'
        if issue_update.issue.status.id != REDMINE_STATUS_ID_RESOLVED:
            issue_update.logger.info(f"Issue status is '{issue_update.issue.status.name}', which is not 'Resolved'.")
            issue_update.logger.info("Updating status to 'Resolved' because its PR is merged.")
            changed = issue_update.change_field('status_id', REDMINE_STATUS_ID_RESOLVED)
            return changed
        else:
            issue_update.logger.info("Issue is already in 'Resolved' status. No change needed.")
            return False

    def _filter_released(self, filters):
        log.debug("Applying _filter_released criteria.")
        filters[f"cf_{REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT}"] = '*'
        filters[f"cf_{REDMINE_CUSTOM_FIELD_ID_RELEASED_IN}"] = '!*'
        return False

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

        if release:
            issue_update.logger.info(f"Commit {commit} is part of release {release}.")
            changed = issue_update.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_RELEASED_IN, release)
            return changed
        elif released_in:
            issue_update.logger.error(f"'Released In' would be cleared (currently: '{released_in}')??")
        else:
            issue_update.logger.info(f"Commit {commit} not yet in a release. 'Released In' field will not be updated.")
        return False

    def _filter_issues_pending_backport(self, filters):
        """
        Filter for issues that are in 'Pending Backport' status.  The
        transformation will then check if they are non-backport trackers and if
        all their 'Copied to' backports are resolved.
        """
        log.debug("Applying _filter_issues_pending_backport criteria.")
        filters["status_id"] = REDMINE_STATUS_ID_PENDING_BACKPORT
        return False

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
                try:
                    # Each transformation method modifies the same issue_update object
                    transform_name = transform_method.__name__.removeprefix("_transform_")
                    issue_update.logger.debug(f"Calling transformation: {transform_name}")
                    issue_update.set_transform(transform_name)
                    if transform_method(issue_update):
                        issue_update.logger.info(f"Transformation {transform_method.__name__} resulted in a change.")
                        applied_transformations.append(transform_method.__name__)
                finally:
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
                except requests.exceptions.HTTPError as err:
                    issue_update.logger.error("API PUT failure during upkeep.", exc_info=True)
                    self._handle_update_failure(issue_update, err)
                    return False
                except Exception as e:
                    issue_update.logger.exception(f"Failed to update Redmine issue during upkeep: {e}")
                    self._handle_update_failure(issue_update, e)
                    return False
            else:
                issue_update.logger.info("No changes detected after all transformations. No Redmine update sent.")
                return False
        finally:
            if IS_GITHUB_ACTION:
                log_stream.flush()
                print(f"::endgroup::", file=sys.stderr, flush=False) # End GitHub Actions group

    def _handle_update_failure(self, issue_update, error):
        """
        Adds a tag and an unsilenced comment to the issue when an update fails.
        """
        issue_id = issue_update.issue.id
        issue_update.logger.error(f"Update failed for issue #{issue_id}. Attempting to add 'upkeep-failed' tag and comment.")

        # Prepare payload for failure update
        failure_payload = {
            'issue': {},
            'suppress_mail': "0", # Do not suppress mail for failure notification
        }

        # Add comment
        failure_payload['issue']['notes'] = f"""
            Redmine Upkeep script failed to update this issue on {datetime.now(timezone.utc).isoformat(timespec='seconds')}.
            Error: {error}
            Payload was:
            <pre>
            {json.dumps(issue_update.get_update_payload(suppress_mail=True), indent=4)}
            </pre>
            """

        # Get existing tags or initialize if none
        current_tags_str = issue_update.get_custom_field(REDMINE_CUSTOM_FIELD_ID_TAGS)
        current_tags = []
        if current_tags_str:
            current_tags = [tag.strip() for tag in current_tags_str.split(',') if tag.strip()]

        new_tag = "upkeep-failed"
        if new_tag in current_tags:
            issue_update.logger.warning(f"'upkeep-failed' tag is already present")
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

        tracker_links = "\n".join([f"https://tracker.ceph.com/issues/{tid}" for tid in found_tracker_ids])
        comment_body = f"""

            This is an automated message by src/script/redmine-upkeep.py.

            I found one or more 'Fixes:' tags in the commit messages in

            `git log {revrange}`

            The referenced tickets are:

            {tracker_links}

            Those tickets do not reference this merged Pull Request. If this
            Pull Request merge resolves any of those tickets, please update the
            "Pull Request ID" field on each ticket. A future run of this
            script will appropriately update them.

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
            "limit": limit,
            "sort": f'cf_{REDMINE_CUSTOM_FIELD_ID_UPKEEP_TIMESTAMP}',
            "status_id": "*",
            f"cf_{REDMINE_CUSTOM_FIELD_ID_TAGS}": "!upkeep-failed",
        }
        #f"cf_{REDMINE_CUSTOM_FIELD_ID_UPKEEP_TIMESTAMP}": f"<={cutoff_date}", # Not updated recently
        log.info("Beginning to loop through shuffled filters.")
        for filter_method in self.filter_methods:
            if limit <= 0:
                log.info("Issue processing limit reached. Stopping filter execution.")
                break
            common_filters['limit'] = limit
            filters = copy.deepcopy(common_filters)
            needs_github_api = filter_method(filters)
            try:
                log.info(f"Running filter {filter_method.__name__} with criteria: {filters}")
                issues = self.R.issue.filter(**filters)
                issue_count = len(issues)
                log.info(f"Filter {filter_method.__name__} returned {issue_count} issue(s).")
                for issue in issues:
                    if needs_github_api and self.GITHUB_RATE_LIMITED:
                        log.warning(f"Stopping filter {filter_method.__name__} due to Github rate limits.")
                        break
                    limit = limit - 1
                    self._process_issue_transformations(issue)
                    if limit <= 0:
                        break
            except redminelib.exceptions.ResourceAttrError as e:
                log.warning(f"Redmine API error with filter {filters}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Ceph redmine upkeep tool")
    parser.add_argument('--debug', dest='debug', action='store_true', help='turn debugging on')
    parser.add_argument('--github-action', dest='gha', action='store_true', help='github action output')
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
        log.info(f"Summary: Issues Inspected: {RU.issues_inspected}, Issues Modified: {RU.issues_modified}")
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
