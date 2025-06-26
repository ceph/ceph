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
import re
import signal
import sys

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

GITHUB_ORG="ceph"
GITHUB_REPO="ceph"

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

log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(logging.INFO)

def gitauth():
    return (GITHUB_USER, GITHUB_TOKEN)

class IssueUpdate:
    def __init__(self, issue, github_session, git_repo):
        self.issue = issue
        self.update_payload = {}
        self.github_session = github_session
        self.git_repo = git_repo
        self._pr_cache = {}
        self.has_changes = False # New flag to track if changes are made

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
            log.debug(f"Field {field_id} is already set to '{value}'. No update needed.")
            return

        for field in custom_fields:
            if field.get('id') == field_id:
                field['value'] = value
                found = True
                break
        if not found:
            custom_fields.append({'id': field_id, 'value': value})
        self.has_changes = True # Mark that a change has been made

    def change_field(self, field, value):
        self.update_payload[field] = value
        self.has_changes = True

    def get_update_payload(self, suppress_mail=True): # Added suppress_mail parameter
        today = datetime.now(timezone.utc).isoformat(timespec='seconds')
        self.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_UPKEEP_TIMESTAMP, today)

        current_tags_str = self.get_custom_field(REDMINE_CUSTOM_FIELD_ID_TAGS)
        current_tags = []
        if current_tags_str:
            current_tags = [tag.strip() for tag in current_tags_str.split(',') if tag.strip()]
            if "upkeep-failed" in current_tags:
                log.info(f"[Issue #%d] 'upkeep-failed' tag found in '%s'. Removing for update.", self.issue.id, current_tags_str)
                current_tags.remove("upkeep-failed")
                self.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_TAGS, ", ".join(current_tags))

        payload = {
            'issue': self.update_payload,
        }
        if suppress_mail:
            payload['suppress_mail'] = "1"
        return payload

    def fetch_pr(self):
        pr_id = self.get_custom_field(REDMINE_CUSTOM_FIELD_ID_PULL_REQUEST_ID)
        if not pr_id:
            log.debug("[Issue #%d] No PR ID found in 'Pull Request ID'.", self.issue.id)

            # If not found in custom field, try to find it in the issue description
            if self.issue.tracker.id == REDMINE_TRACKER_ID_BACKPORT and self.issue.description:
                match = re.search(r'^https://github.com/ceph/ceph/pull/(\d+)$', self.issue.description.strip())
                if match:
                    pr_id = match.group(1)

        try:
            pr_id = int(pr_id)
        except ValueError:
            log.warning("[Issue #%d] Invalid PR ID '%s'.", self.issue.id, pr_id)
            return None

        if pr_id in self._pr_cache:
            return self._pr_cache[pr_id]

        endpoint = f"https://api.github.com/repos/{GITHUB_ORG}/{GITHUB_REPO}/pulls/{pr_id}"
        params = {}
        try:
            response = self.github_session.get(endpoint, auth=gitauth(), params=params)
            response.raise_for_status()
            pr_data = response.json()
            log.debug("PR #%d json:\n%s", pr_id, pr_data)
            self._pr_cache[pr_id] = pr_data
            return pr_data
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                log.warning(f"GitHub PR #{pr_id} not found")
            elif response.status_code == 429:
                log.warning(f"GitHub is throttling, quitting!")
                sys.exit(2)
            else:
                log.error(f"GitHub API error for PR #{pr_id}: {e} - Response: {response.text}")
            return None
        except requests.exceptions.RequestException as e:
            log.error(f"Network or request error fetching GitHub PR #{pr_id}: {e}")
            return None

    def get_released(self, commit):
        """
        Determines the release version a commit is part of.
        """
        try:
            release = self.git_repo.git.describe('--contains', '--match', 'v*.2.*', commit)
            log.debug("[Issue #%d] release should be %s", self.issue.id, release)
            patt = r"v(\d+)\.(\d+)\.(\d+)"
            match = re.search(patt, release)
            if not match:
                log.warning("[Issue #%d] release is invalid form", self.issue.id)
                return None
            if int(match.group(2)) != 2:
                log.debug("[Issue #%d] release is not a valid release (minor version not 2)", self.issue.id)
                return None
            return release
        except git.exc.GitCommandError:
            log.debug("[Issue #%d] Commit %s not found in any matching tag.", self.issue.id, commit)
            return None
        except Exception as e:
            log.error(f"[Issue #%d] Error in get_released for commit {commit}: {e}", self.issue.id)
            return None

class RedmineUpkeep:
    def __init__(self, args):
        self.G = git.Repo(args.git)
        self.R = self._redmine_connect()
        self.limit = args.limit
        self.session = requests.Session()
        self.issue_id = args.issue # Store issue_id from args
        self.revision_range = args.revision_range # Store revision_range from args

        self.project_id = None
        try:
            project = self.R.project.get("Ceph")
            self.project_id = project['id']
        except redminelib.exceptions.ResourceAttrError:
            log.error("Project 'Ceph' not found in Redmine. Cannot filter issues by project.")
            sys.exit(1)

        # Discover transformation methods based on prefix
        self.transform_methods = []
        for name in dir(self):
            if name.startswith('_transform_') and callable(getattr(self, name)):
                self.transform_methods.append(getattr(self, name))

        # Sort transformations for consistent order if needed, e.g., by name
        self.transform_methods.sort(key=lambda x: x.__name__)

        # Discover filter methods based on prefix
        self.filter_methods = []
        for name in dir(self):
            if name.startswith('_filter_') and callable(getattr(self, name)):
                self.filter_methods.append(getattr(self, name))

        # Sort filters for consistent order if needed, e.g., by name
        self.filter_methods.sort(key=lambda x: x.__name__)

    def _redmine_connect(self):
        log.info("connecting to %s", REDMINE_ENDPOINT)
        R = redminelib.Redmine(REDMINE_ENDPOINT, key=REDMINE_API_KEY)
        log.debug("connected")
        return R

    # Transformations:

    def _filter_merged(self, filters):
        filters[f"cf_{REDMINE_CUSTOM_FIELD_ID_PULL_REQUEST_ID}"] = '>=0'
        filters[f"cf_{REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT}"] = '!*'
        filters["status_id"] = [
            REDMINE_STATUS_ID_PENDING_BACKPORT,
            REDMINE_STATUS_ID_RESOLVED
        ]
        return filters

    def _transform_merged(self, issue_update):
        """
        Transformation: Checks if a PR associated with an issue has been merged
        and updates the merge commit and fixed_in fields in the payload.
        """
        log.info("[Issue #%d] Running _transform_merged", issue_update.issue.id)

        commit = issue_update.get_custom_field(REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT)
        if not commit:
            pr = issue_update.fetch_pr()
            if not pr:
                log.debug("[Issue #%d] No PR data found. Skipping merge check.", issue_update.issue.id)
                return

            merged = pr.get('merged')
            if not merged:
                log.debug("[Issue #%d] PR #%s not merged. Skipping merge check.", issue_update.issue.id, pr['number'])
                return

            commit = pr.get('merge_commit_sha')
            if not commit:
                log.debug("[Issue #%d] PR #%s has no merge commit SHA. Skipping merge check.", issue_update.issue.id, pr['number'])
                return

            log.info("[Issue #%d] PR #%s merged with commit %s", issue_update.issue.id, pr['number'], commit)

            issue_update.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT, commit)

        try:
            ref = issue_update.git_repo.git.describe('--always', commit)
            issue_update.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_FIXED_IN, ref)
        except git.exc.GitCommandError as e:
            log.warning("[Issue #%d] Could not get git describe for commit %s: %s", issue_update.issue.id, commit, e)

    def _transform_backport_resolved(self, issue_update):
        """
        Transformation: Changes backport trackers to "Resolved" if the associated PR is merged.
        """
        log.info("[Issue #%d] Running _transform_backport_resolved", issue_update.issue.id)

        # Check if it's a backport tracker
        if issue_update.issue.tracker.id != REDMINE_TRACKER_ID_BACKPORT:
            log.debug("[Issue #%d] Not a backport tracker. Skipping backport resolved check.", issue_update.issue.id)
            return

        commit = issue_update.get_custom_field(REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT)
        if not commit:
            # Check if the current status is 'Pending Backport'
            if issue_update.issue.status.id == REDMINE_STATUS_ID_RESOLVED:
                log.debug("[Issue #%d] Status is 'Resolved'. Skipping backport resolved check.", issue_update.issue.id)
                return

            pr = issue_update.fetch_pr()
            if not pr:
                log.debug("[Issue #%d] No PR data found. Skipping backport resolved check.", issue_update.issue.id)
                return

            merged = pr.get('merged')
            if not merged:
                log.debug("[Issue #%d] PR #%s not merged. Skipping backport resolved check.", issue_update.issue.id, pr['number'])
                return

        # If PR is merged and it's a backport tracker with 'Pending Backport' status, update to 'Resolved'
        if issue_update.issue.status.id != REDMINE_STATUS_ID_RESOLVED:
            log.debug("[Issue #%d] Issue status was '%s'.", issue_update.issue.id, issue_update.issue.status)
            log.info("[Issue #%d] Updating status to 'Resolved'.", issue_update.issue.id)
            issue_update.change_field('status_id', REDMINE_STATUS_ID_RESOLVED)
        else:
            log.debug("[Issue #%d] Issue already in 'Resolved' status. No change needed.", issue_update.issue.id)

    def _filter_released(self, filters):
        filters[f"cf_{REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT}"] = '*'
        filters[f"cf_{REDMINE_CUSTOM_FIELD_ID_RELEASED_IN}"] = '!*'
        return filters

    def _transform_released(self, issue_update):
        """
        Transformation: Checks if a merged issue has been released and updates
        the 'Released In' field in the payload.
        """
        log.info("[Issue #%d] Running _transform_released", issue_update.issue.id)
        commit = issue_update.get_custom_field(REDMINE_CUSTOM_FIELD_ID_MERGE_COMMIT)
        if not commit:
            log.debug("[Issue #%d] No merge commit set. Skipping released check.", issue_update.issue.id)
            return

        released_in = issue_update.get_custom_field(REDMINE_CUSTOM_FIELD_ID_RELEASED_IN)
        log.debug("[Issue #%d] 'Released In' currently '%s'", issue_update.issue.id, released_in)

        release = issue_update.get_released(commit)

        if release:
            issue_update.add_or_update_custom_field(REDMINE_CUSTOM_FIELD_ID_RELEASED_IN, release)
        elif released_in:
            log.error("[Issue #%d] 'Released In' would be cleared (currently: '%s')??", issue_update.issue.id, released_in)

    def _filter_issues_pending_backport(self, filters):
        """
        Filter for issues that are in 'Pending Backport' status.  The
        transformation will then check if they are non-backport trackers and if
        all their 'Copied to' backports are resolved.
        """
        filters["status_id"] = REDMINE_STATUS_ID_PENDING_BACKPORT
        return filters

    def _transform_resolve_main_issue_from_backports(self, issue_update):
        """
        Transformation: Resolves a main issue if all its "Copied to" backport
        issues are resolved and correctly tagged with the expected backport
        releases.
        """
        log.info("[Issue #%d] Running _transform_resolve_main_issue_from_backports", issue_update.issue.id)

        if issue_update.issue.tracker.id == REDMINE_TRACKER_ID_BACKPORT:
            log.debug("[Issue #%d] Is a backport tracker. Skipping this transformation.", issue_update.issue.id)
            return

        if issue_update.issue.status.id != REDMINE_STATUS_ID_PENDING_BACKPORT:
            log.debug("[Issue #%d] Not in 'Pending Backport' status (%s). Skipping.", issue_update.issue.id, issue_update.issue.status.name)
            return

        log.debug("[Issue #%d] Is in 'Pending Backport' status. Checking related backports.", issue_update.issue.id)

        expected_backport_releases_str = issue_update.get_custom_field(REDMINE_CUSTOM_FIELD_ID_BACKPORT)
        if expected_backport_releases_str:
            expected_backport_releases = set(
                rel.strip() for rel in expected_backport_releases_str.split(',') if rel.strip()
            )
        else:
            expected_backport_releases = set()
            log.debug("[Issue #%d] No backport releases specified in custom field %d.",
                      issue_update.issue.id, REDMINE_CUSTOM_FIELD_ID_BACKPORT)

        copied_to_backports_ids = []
        try:
            # Fetch the issue again with 'include=relations' to ensure relations are loaded
            issue_with_relations = self.R.issue.get(issue_update.issue.id, include=['relations'])

            for relation in issue_with_relations.relations:
                if relation.relation_type == 'copied_to':
                    copied_to_backports_ids.append(relation.issue_to_id)
        except redminelib.exceptions.ResourceAttrError as e:
            log.warning(f"[Issue #%d] Could not fetch relations for issue: {e}. Skipping backport status check.", issue_update.issue.id)
            return
        except Exception as e:
            log.exception(f"[Issue #%d] Error fetching relations: {e}", issue_update.issue.id)
            return

        if not copied_to_backports_ids and not expected_backport_releases:
            # If no backports are expected and no 'Copied to' issues exist,
            # then the main issue can be resolved.
            log.info("[Issue #%d] No backports expected and no 'Copied to' issues found. Updating main issue status to 'Resolved'.",
                     issue_update.issue.id)
            issue_update.change_field('status_id', REDMINE_STATUS_ID_RESOLVED)
            return
        elif not copied_to_backports_ids and expected_backport_releases:
            # If backports are expected but no 'Copied to' issues exist,
            # the main issue cannot be resolved.
            log.debug("[Issue #%d] Backports expected (%s) but no 'Copied to' issues found. Main issue cannot be resolved.",
                      issue_update.issue.id, ", ".join(expected_backport_releases))
            return

        resolved_and_matched_backports = set()
        all_backports_resolved_and_matched = True

        for backport_id in copied_to_backports_ids:
            try:
                log.debug("[Issue #%d] Looking up backport issue #%d", issue_update.issue.id, backport_id)
                backport_issue = self.R.issue.get(backport_id)
                
                # Ensure the related issue is actually a backport tracker
                if backport_issue.tracker.id != REDMINE_TRACKER_ID_BACKPORT:
                    log.warning("[Issue #%d] Related issue #%d is 'Copied to' but not a backport tracker. Ignoring it for resolution check.",
                                issue_update.issue.id, backport_id)
                    continue

                # Check backport issue's release field
                cf_backport_release = backport_issue.custom_fields.get(REDMINE_CUSTOM_FIELD_ID_RELEASE)
                if not cf_backport_release:
                    log.debug("[Issue #%d] Backport issue #%d has no release specified in custom field %d. Cannot resolve main issue yet.",
                              issue_update.issue.id, backport_id, REDMINE_CUSTOM_FIELD_ID_RELEASE)
                    all_backports_resolved_and_matched = False
                    break

                backport_release = cf_backport_release.value
                if backport_release not in expected_backport_releases:
                    log.debug("[Issue #%d] Backport issue #%d has release '%s' which is not in expected backports (%s). Main issue cannot be resolved yet.",
                              issue_update.issue.id, backport_id, backport_release, ", ".join(expected_backport_releases))
                    all_backports_resolved_and_matched = False
                    break

                if backport_issue.status.id == REDMINE_STATUS_ID_RESOLVED:
                    log.debug("[Issue #%d] Backport issue #%d is resolved and matches expected release '%s'.",
                              issue_update.issue.id, backport_id, backport_release)
                    resolved_and_matched_backports.add(backport_release)
                elif backport_issue.status.id == REDMINE_STATUS_ID_REJECTED:
                    log.debug("[Issue #%d] Backport issue #%d is rejected and matches expected release '%s'.",
                              issue_update.issue.id, backport_id, backport_release)
                    resolved_and_matched_backports.add(backport_release)
                else:
                    log.debug("[Issue #%d] Backport issue #%d is not resolved (status: %s). Main issue cannot be resolved yet.",
                              issue_update.issue.id, backport_id, backport_issue.status.name)
                    all_backports_resolved_and_matched = False
                    break
            except redminelib.exceptions.ResourceAttrError:
                log.warning(f"[Issue #%d] Related backport issue #{backport_id} not found or accessible. Cannot confirm all backports resolved.", issue_update.issue.id)
                all_backports_resolved_and_matched = False # Treat as not resolved if we can't find it
                break
            except Exception as e:
                log.exception(f"[Issue #%d] Error fetching backport issue #{backport_id}: {e}", issue_update.issue.id)
                all_backports_resolved_and_matched = False # Treat as not resolved if there's an error
                break

        # Final check: all backports found, resolved, correctly tagged, and all expected backports are covered
        if all_backports_resolved_and_matched and expected_backport_releases == resolved_and_matched_backports:
            log.info("[Issue #%d] All expected backport releases (%s) have corresponding resolved and correctly tagged 'Copied to' issues. Updating main issue status to 'Resolved'.",
                     issue_update.issue.id, ", ".join(expected_backport_releases))
            issue_update.change_field('status_id', REDMINE_STATUS_ID_RESOLVED)
        else:
            log.debug("[Issue #%d] Not all expected backports are resolved and/or correctly tagged. Main issue status remains 'Pending Backport'.", issue_update.issue.id)
            log.debug("[Issue #%d] Expected backports: %s", issue_update.issue.id, expected_backport_releases)
            log.debug("[Issue #%d] Resolved and matched backports found: %s", issue_update.issue.id, resolved_and_matched_backports)

    def _process_issue_transformations(self, issue):
        """
        Applies all discovered transformation methods to a single Redmine issue
        and sends a single update API call if changes are made.
        """
        log.info("[Issue #%d] Processing issue: '%s' %s", issue.id, issue.subject, issue.url)
        issue_update = IssueUpdate(issue, self.session, self.G)

        for transform_method in self.transform_methods:
            try:
                # Each transformation method modifies the same issue_update object
                transform_method(issue_update)
            except Exception as e:
                log.exception(f"[Issue #%d] Error applying transformation {transform_method.__name__}: {e}", issue.id)

        if issue_update.has_changes:
            log.info("[Issue #%d] Changes detected. Sending update to Redmine...", issue.id)
            try:
                # We cannot put top-level changes in the PUT request against
                # the redmine API via redminelib. So we send it manually.
                payload = issue_update.get_update_payload()
                log.debug("[Issue #%d] PUT payload:\n%s", issue.id, json.dumps(payload, indent=4))
                headers = {
                    'Content-Type': 'application/json',
                    'X-Redmine-API-Key': REDMINE_API_KEY,
                }
                endpoint = f"{REDMINE_ENDPOINT}/issues/{issue.id}.json"
                response = requests.put(endpoint, headers=headers, data=json.dumps(payload))
                response.raise_for_status()
                log.info(f"[Issue #%d] Successfully updated Redmine issue.", issue.id)
                return True
            except requests.exceptions.HTTPError as err:
                log.exception("[Issue #%d] API PUT failure during upkeep.", issue.id)
                self._handle_update_failure(issue_update, err)
                return False
            except Exception as e:
                log.exception(f"[Issue #%d] Failed to update Redmine issue during upkeep: {e}", issue.id)
                self._handle_update_failure(issue_update, e)
                return False
        else:
            log.info("[Issue #%d] No changes detected. No Redmine update sent.", issue.id)
            return False

    def _handle_update_failure(self, issue_update, error):
        """
        Adds a tag and an unsilenced comment to the issue when an update fails.
        """
        issue_id = issue_update.issue.id
        log.warning(f"[Issue #%d] Attempting to add 'upkeep-failed' tag and comment due to update failure.", issue_id)

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
            log.warning(f"[Issue #%d] 'upkeep-failed' tag is already present", issue_id)
        else:
            current_tags.append(new_tag)

        # Update custom field for tags in the failure payload
        custom_fields_payload = failure_payload['issue'].setdefault('custom_fields', [])
        custom_fields_payload.append(
            {'id': REDMINE_CUSTOM_FIELD_ID_TAGS, 'value': ", ".join(current_tags)}
        )

        try:
            log.info("[Issue #%d] Sending failure update payload to Redmine...", issue_id)
            headers = {
                'Content-Type': 'application/json',
                'X-Redmine-API-Key': REDMINE_API_KEY,
            }
            endpoint = f"{REDMINE_ENDPOINT}/issues/{issue_id}.json"
            response = requests.put(endpoint, headers=headers, data=json.dumps(failure_payload))
            response.raise_for_status()
            log.info(f"[Issue #%d] Successfully added 'upkeep-failed' tag and comment to Redmine issue.", issue_id)
        except requests.exceptions.HTTPError as err:
            log.fatal(f"[Issue #%d] Could not update Redmine issue with failure tag/comment: {err} - Response: {response.text}", issue_id)
            sys.exit(1)
        except Exception as e:
            log.fatal(f"[Issue #%d] An unexpected error occurred while trying to update Redmine issue with failure tag/comment: {e}", issue_id)
            sys.exit(1)

    def filter_and_process_issues(self):
        """
        Fetches issues based on filters or revision range/specific issue ID and
        processes each one using all registered transformations.
        """
        if self.issue_id is not None:
            try:
                issue = self.R.issue.get(self.issue_id)
                self._process_issue_transformations(issue)
            except redminelib.exceptions.ResourceAttrError:
                log.error(f"Issue #{self.issue_id} not found in Redmine.")
                sys.exit(1)
            except Exception as e:
                log.exception(f"Error fetching or processing issue #{self.issue_id}: {e}")
                sys.exit(1)
        elif self.revision_range is not None:
            self._execute_revision_range()
        else:
            self._execute_filters()

    def _execute_revision_range(self):
        log.info(f"Processing issues based on revision range: {self.revision_range}")
        try:
            # Get first-parent merge commits in the revision range
            merge_commits = self.G.git.log(
                '--first-parent',
                '--merges',
                '--pretty=%H',
                self.revision_range
            ).splitlines()
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
                    for issue in issues:
                        self._process_issue_transformations(issue)
                except redminelib.exceptions.ResourceAttrError as e:
                    log.error(f"Redmine API error for merge commit {commit}: {e}")
                    raise
                except Exception as e:
                    log.exception(f"Error processing issues for merge commit {commit}: {e}")
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

        for filter_method in self.filter_methods:
            if limit <= 0:
                break
            try:
                common_filters['limit'] = limit
                filter_kwargs = filter_method(copy.deepcopy(common_filters))
                log.info(f"Running filter {filter_method.__name__} with criteria: {filter_kwargs}")
                try:
                    issues = self.R.issue.filter(**filter_kwargs)
                    for issue in issues:
                        limit = limit - 1
                        self._process_issue_transformations(issue)
                        if limit <= 0:
                            break
                except redminelib.exceptions.ResourceAttrError as e:
                    log.warning(f"Redmine API error with filter {filter_kwargs}: {e}")
                except Exception as e:
                    log.exception(f"Error filtering or processing issues with filter {filter_kwargs}: {e}")
            except Exception as e:
                log.exception(f"Error applying transformation {filter_method.__name__}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Ceph redmine upkeep tool")
    parser.add_argument('--debug', dest='debug', action='store_true', help='turn debugging on')
    parser.add_argument('--limit', dest='limit', action='store', type=int, default=25, help='limit processed issues')
    parser.add_argument('--git-dir', dest='git', action='store', default=".", help='git directory')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--issue', dest='issue', action='store', help='issue to check')
    group.add_argument('--revision-range', dest='revision_range', action='store',
                       help='Git revision range (e.g., "v12.2.2..v12.2.3") to find merge commits and process related issues.')

    args = parser.parse_args(sys.argv[1:])

    if args.debug:
        log.setLevel(logging.DEBUG)
        git_logger = logging.getLogger('git.cmd')
        git_logger.setLevel(logging.DEBUG)
        git_logger.addHandler(logging.StreamHandler(sys.stderr))
        requests_logger = logging.getLogger("requests.packages.urllib3")
        requests_logger.setLevel(logging.DEBUG)
        requests_logger.propagate = True

    if not REDMINE_API_KEY:
        log.fatal("REDMINE_API_KEY not found! Please set REDMINE_API_KEY environment variable or ~/.redmine_key.")
        sys.exit(1)

    try:
        RU = RedmineUpkeep(args)
        RU.filter_and_process_issues() # No arguments needed here anymore
    except Exception as e:
        log.fatal(f"An unhandled error occurred during Redmine upkeep: {e}", exc_info=True)
        sys.exit(1)

    sys.exit(0)

if __name__ == "__main__":
    main()
