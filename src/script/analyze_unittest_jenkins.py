#!/usr/bin/env python3
"""
Analyze unit test results across Ceph PRs on Jenkins.

This script:
1. Fetches recent open PRs from GitHub
2. Extracts Jenkins build URLs from PR checks
3. Downloads console logs
4. Parses for unit test results (all tests or filtered by pattern)
5. Generates statistics and failure reports

Usage:
    python3 analyze_unittest_jenkins.py --help
    python3 analyze_unittest_jenkins.py [--max-prs 100] [--output report.json]
    python3 analyze_unittest_jenkins.py --test-filter unittest_mgr --max-prs 50
    python3 analyze_unittest_jenkins.py --release reef --max-prs 30
    python3 analyze_unittest_jenkins.py --release main --test-filter unittest_mgr
    
Requirements:
    pip install requests beautifulsoup4
"""

import argparse
import json
import os
import re
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from itertools import islice
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, TextIO, Tuple
from urllib.parse import urljoin

try:
    import requests
    from requests.adapters import HTTPAdapter
    from bs4 import BeautifulSoup
except ImportError:
    print("Error: Required packages not installed.")
    print("Please run: pip install requests beautifulsoup4")
    sys.exit(1)


@dataclass(frozen=True)
class TestOutcome:
    """Configuration for a test outcome type.
    
    Attributes:
        counter_key: The results dictionary key to increment ('passed' or 'failed')
        is_failure: Whether this outcome represents a test failure
        suffix: Optional suffix to append to failed test names (e.g., ' (timeout)')
    """
    counter_key: str
    is_failure: bool
    suffix: Optional[str] = None


class JenkinsAnalyzer:
    """Analyzes unit test results from Jenkins builds."""
    
    # API Configuration Constants
    DEFAULT_PER_PAGE = 100
    REQUEST_TIMEOUT = 30
    MAX_RETRY_ATTEMPTS = 3
    MIN_404_THRESHOLD = 3  # Minimum 404s before early termination
    
    def __init__(
        self,
        github_token: Optional[str] = None,
        max_workers: int = 10,
        test_filter: Optional[str] = None,
        verbose: bool = True,
        log_file: Optional[str] = None
    ) -> None:
        """Initialize Jenkins analyzer.
        
        Args:
            github_token: GitHub API token for higher rate limits
            max_workers: Number of parallel workers for downloads
            test_filter: Optional test name pattern to filter results
            verbose: Enable verbose console output
            log_file: Optional file path for detailed logging
        """
        self.github_token: Optional[str] = github_token
        self.max_workers: int = max_workers
        self.test_filter: Optional[str] = test_filter
        self.verbose: bool = verbose
        self.log_file: Optional[str] = log_file
        self.log_lock: threading.Lock = threading.Lock()
        self.session: requests.Session = requests.Session()
        self.log_fp: Optional[TextIO] = None
        
        # Configure session with connection pooling for better performance
        if github_token:
            self.session.headers['Authorization'] = f'token {github_token}'
        
        # Enable connection pooling with larger pool size
        adapter = HTTPAdapter(
            pool_connections=max_workers * 2,
            pool_maxsize=max_workers * 4,
            max_retries=0  # We handle retries manually
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        # Open log file with proper encoding
        if log_file:
            self.log_fp = Path(log_file).open('w', encoding='utf-8')
        
        # Build test parsing patterns
        self.test_patterns: Dict[str, re.Pattern] = self._build_test_patterns(test_filter)
    
    def _build_test_patterns(self, test_filter: Optional[str]) -> Dict[str, re.Pattern]:
        """Build regex patterns for parsing CTest console output.
        
        Note: There's NO space between dots and ***Failed/***Timeout/etc.
        
        Args:
            test_filter: Optional test name pattern to filter results
            
        Returns:
            Dictionary of compiled regex patterns for parsing test output
        """
        if test_filter:
            # Filter for specific test pattern
            test_pattern = rf'({re.escape(test_filter)}\S*)'
        else:
            # Capture ALL tests (no filter)
            test_pattern = r'(\S+)'
        
        return {
            'test_start': re.compile(rf'Start\s+\d+:\s+{test_pattern}'),
            'test_passed': re.compile(rf'\d+/\d+\s+Test\s+#\d+:\s+{test_pattern}\s+\.+\s+Passed\s+([\d.]+)\s+sec'),
            'test_failed': re.compile(rf'\d+/\d+\s+Test\s+#\d+:\s+{test_pattern}\s+\.+\*\*\*Failed\s+([\d.]+)\s+sec'),
            'test_timeout': re.compile(rf'\d+/\d+\s+Test\s+#\d+:\s+{test_pattern}\s+\.+\*\*\*Timeout\s+([\d.]+)\s+sec'),
            'test_exception': re.compile(rf'\d+/\d+\s+Test\s+#\d+:\s+{test_pattern}\s+\.+\*\*\*Exception:\s+'),
            'test_not_run': re.compile(rf'\d+/\d+\s+Test\s+#\d+:\s+{test_pattern}\s+\.+\*\*\*Not Run\s+'),
            'summary': re.compile(r'(\d+)%\s+tests\s+passed,\s+(\d+)\s+tests\s+failed\s+out\s+of\s+(\d+)'),
        }
    
    def log(self, message: str, force_print: bool = False, thread_id: Optional[int] = None):
        """Log message to file and/or console based on verbosity settings.
        
        Args:
            message: The message to log
            force_print: Force printing even if verbose is False
            thread_id: Optional thread ID to include in the log message
        """
        if thread_id is not None:
            formatted_message = f"  [TID:{thread_id}] {message}"
        else:
            formatted_message = message
            
        with self.log_lock:
            if self.log_fp:
                self.log_fp.write(formatted_message + '\n')
                self.log_fp.flush()
            if self.verbose or force_print:
                print(formatted_message)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with automatic cleanup."""
        self.close()
        return False
    
    def close(self):
        """Close log file if open."""
        if self.log_fp and not self.log_fp.closed:
            self.log_fp.close()
            self.log_fp = None
    
    def _fetch_with_retry(
        self, 
        url: str, 
        params: Dict[str, Any],
        max_retries: Optional[int] = None
    ) -> Optional[requests.Response]:
        """Fetch URL with exponential backoff retry logic.
        
        Args:
            url: URL to fetch
            params: Query parameters
            max_retries: Maximum number of retry attempts (defaults to class constant)
            
        Returns:
            Response object or None if all retries failed
        """
        if max_retries is None:
            max_retries = self.MAX_RETRY_ATTEMPTS
            
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=self.REQUEST_TIMEOUT)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt == max_retries - 1:
                    self.log(f"  Error fetching PRs after {max_retries} attempts: {e}")
                    return None
                
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                self.log(f"  Request failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
        
        return None
    
    def _fetch_pr_pages(self, per_page: Optional[int] = None) -> Generator[List[Dict], None, None]:
        """Generator that yields pages of open PRs from GitHub API.
        
        Args:
            per_page: Number of PRs per page (defaults to class constant)
            
        Yields:
            List of PR dictionaries from each API page
        """
        if per_page is None:
            per_page = self.DEFAULT_PER_PAGE
            
        page = 1
        url = "https://api.github.com/repos/ceph/ceph/pulls"
        
        while True:
            params = {
                'state': 'open',
                'sort': 'updated',
                'direction': 'desc',
                'per_page': per_page,
                'page': page
            }
            
            response = self._fetch_with_retry(url, params)
            if response is None:
                break
                
            page_prs = response.json()
            if not page_prs:
                break
                
            yield page_prs
            page += 1
    
    def fetch_recent_prs(self, max_prs: int = 100, release: str = "main") -> List[Dict]:
        """Fetch recent open PRs from GitHub, filtering by target release branch.
        
        Args:
            max_prs: Maximum number of PRs to fetch
            release: Target release branch to filter by
            
        Returns:
            List of PR dictionaries matching the release filter
        """
        self.log("Fetching recent open PRs from GitHub...", force_print=True)

        filtered_prs: List[Dict] = []
        total_fetched = 0

        for page_num, page_prs in enumerate(self._fetch_pr_pages(), start=1):
            total_fetched += len(page_prs)
            
            # Filter PRs by release and collect up to remaining quota
            matching_prs = (
                pr for pr in page_prs
                if pr.get('base', {}).get('ref') == release
            )
            remaining = max_prs - len(filtered_prs)
            new_prs = list(islice(matching_prs, remaining))
            filtered_prs.extend(new_prs)

            self.log(
                f"  Fetched page {page_num}: {len(page_prs)} open PRs "
                f"({len(new_prs)} targeting '{release}', "
                f"{len(filtered_prs)} collected so far)"
            )

            if len(filtered_prs) >= max_prs:
                break

        self.log(
            f"Fetched {total_fetched} open PRs, returning "
            f"{len(filtered_prs)} targeting '{release}'",
            force_print=True
        )
        return filtered_prs
    
    def _should_skip_build(self, name: str, skip_builds: set) -> bool:
        """Check if a build should be skipped based on its name.
        
        Args:
            name: The build name to check
            skip_builds: Set of build name patterns to skip
            
        Returns:
            True if the build should be skipped, False otherwise
        """
        if not skip_builds:
            return False
        name_lower = name.lower()
        return any(skip.lower() in name_lower for skip in skip_builds)
    
    def _get_skip_builds(self) -> set:
        """Get the set of build names to skip based on test filter.
        
        These builds never contain unit tests, so we skip them to save time.
        
        Returns:
            Set of build name patterns to skip
        """
        # Always skip these - they're not actual builds
        always_skip = {'Signed-off-by', 'Unmodified Submodules'}
        
        # These builds never run unit tests, so always skip them
        no_unittest_builds = {
            'ceph API tests',
            'ceph windows tests',
            'ceph dashboard tests',
            'ceph dashboard cephadm e2e tests'
        }
        
        return always_skip | no_unittest_builds
    
    def _fetch_check_runs(self, pr_data: Dict) -> List[Dict]:
        """Fetch check-runs for a PR using its head commit SHA.
        
        Args:
            pr_data: The PR data dictionary containing head commit info
            
        Returns:
            List of check-run dictionaries, or empty list on error
        """
        pr_number = pr_data.get('number')
        head_sha = pr_data.get('head', {}).get('sha')
        
        if not head_sha:
            self.log(f"  No head SHA found for PR #{pr_number}")
            return []
            
        url = f"https://api.github.com/repos/ceph/ceph/commits/{head_sha}/check-runs"
        try:
            response = self.session.get(url, timeout=30)
            if response.status_code == 200:
                return response.json().get('check_runs', [])
        except requests.RequestException as e:
            self.log(f"  Error fetching check-runs for PR #{pr_number}: {e}")
        return []
    
    def _fetch_statuses(self, pr_number: int) -> List[Dict]:
        """Fetch statuses for a PR.
        
        Args:
            pr_number: The PR number to fetch statuses for
            
        Returns:
            List of status dictionaries, or empty list on error
        """
        try:
            pr_url = f"https://api.github.com/repos/ceph/ceph/pulls/{pr_number}"
            response = self.session.get(pr_url, timeout=30)
            response.raise_for_status()
            pr_data = response.json()
            
            statuses_url = pr_data.get('statuses_url')
            if statuses_url:
                response = self.session.get(statuses_url, timeout=30)
                if response.status_code == 200:
                    return response.json()
        except requests.RequestException as e:
            self.log(f"  Error fetching statuses for PR #{pr_number}: {e}")
        return []
    
    def _extract_urls_from_check_runs(self, check_runs: List[Dict], skip_builds: set) -> Dict[str, str]:
        """Extract Jenkins URLs from GitHub check-runs.
        
        Args:
            check_runs: List of check-run dictionaries from GitHub API
            skip_builds: Set of build name patterns to skip
            
        Returns:
            Dictionary mapping build names to Jenkins URLs
        """
        return {
            check['name']: check['details_url']
            for check in check_runs
            if 'jenkins.ceph.com' in check.get('details_url', '')
            and not self._should_skip_build(check.get('name', ''), skip_builds)
        }
    
    def _extract_urls_from_statuses(self, statuses: List[Dict], skip_builds: set, existing_urls: Dict[str, str]) -> Dict[str, str]:
        """Extract Jenkins URLs from GitHub statuses.
        
        Args:
            statuses: List of status dictionaries from GitHub API
            skip_builds: Set of build name patterns to skip
            existing_urls: Dictionary of already-extracted URLs to avoid duplicates
            
        Returns:
            Dictionary mapping build contexts to Jenkins URLs
        """
        return {
            status['context']: status['target_url']
            for status in statuses
            if 'jenkins.ceph.com' in status.get('target_url', '')
            and status.get('context', '') not in existing_urls
            and not self._should_skip_build(status.get('context', ''), skip_builds)
        }
    
    def extract_jenkins_urls(self, pr_data: Dict) -> Dict[str, str]:
        """Extract Jenkins build URLs from PR checks using both check-runs and statuses APIs.
        
        Args:
            pr_data: The PR data dictionary from GitHub API
            
        Returns:
            Dictionary mapping build names to Jenkins URLs
        """
        pr_number = pr_data.get('number', 0)
        skip_builds = self._get_skip_builds()
        
        # Fetch and extract from check-runs API (newer PRs)
        check_runs = self._fetch_check_runs(pr_data)
        jenkins_urls = self._extract_urls_from_check_runs(check_runs, skip_builds)
        
        # Fetch and extract from statuses API (older PRs)
        if pr_number:
            statuses = self._fetch_statuses(pr_number)
            jenkins_urls.update(
                self._extract_urls_from_statuses(statuses, skip_builds, jenkins_urls)
            )
        
        return jenkins_urls
    
    def download_console_log(self, jenkins_url: str) -> tuple[Optional[str], bool]:
        """Download console log from Jenkins build.
        
        Returns:
            tuple: (console_log_text, is_404)
        """
        console_url = jenkins_url.rstrip('/') + '/consoleText'
        
        try:
            response = requests.get(console_url, timeout=60)
            response.raise_for_status()
            return response.text, False
        except requests.HTTPError as e:
            # Track 404 errors for early termination logic
            if e.response.status_code == 404:
                return None, True
            else:
                self.log(f"Error downloading console log: {e}", thread_id=threading.get_ident())
                return None, False
        except requests.RequestException as e:
            self.log(f"Error downloading console log: {e}", thread_id=threading.get_ident())
            return None, False
    
    def parse_test_results(self, console_log: str) -> Dict[str, Any]:
        """Parse test results from CTest console log with optimized single-pass parsing.
        
        Uses dataclass-based outcome configuration for type safety and performance.
        Eliminates repeated dictionary lookups by using immutable TestOutcome objects.
        """
        # Outcome configuration using dataclass for type safety and performance
        OUTCOME_CONFIGS = {
            'test_passed': TestOutcome('passed', False),
            'test_failed': TestOutcome('failed', True),
            'test_timeout': TestOutcome('failed', True, ' (timeout)'),
            'test_exception': TestOutcome('failed', True, ' (exception)'),
            'test_not_run': TestOutcome('failed', True, ' (not run)')
        }
        
        results = {
            'found': False,
            'test_suites': set(),
            'total_tests': 0,
            'passed': 0,
            'failed': 0,
            'failed_tests': [],
            'execution_time_ms': 0
        }
        
        tracked_tests = set()
        
        for line in console_log.splitlines():
            # Parse test start - collect all matching tests
            match = self.test_patterns['test_start'].search(line)
            if match:
                tracked_tests.add(match.group(1))
                results['found'] = True
                continue
            
            # Single-pass outcome matching with early exit
            for pattern_key, outcome in OUTCOME_CONFIGS.items():
                match = self.test_patterns[pattern_key].search(line)
                if match:
                    test_name = match.group(1)
                    if test_name in tracked_tests:
                        # Update counter using dataclass field
                        results[outcome.counter_key] += 1
                        
                        # Add to failed tests if this is a failure outcome
                        if outcome.is_failure:
                            suffix = outcome.suffix or ''
                            results['failed_tests'].append(f"{test_name}{suffix}")
                        
                        # Track unique test suite
                        results['test_suites'].add(test_name)
                    break  # Outcomes are mutually exclusive
            
            # Parse summary line (preserved for potential future use)
            if self.test_patterns['summary'].search(line) and results['found']:
                pass  # Summary already captured via per-test counts
        
        # Final conversions
        results['test_suites'] = list(results['test_suites'])
        results['total_tests'] = results['passed'] + results['failed']
        
        return results
    
    def _process_build(self, context_url: Tuple[str, str]) -> Tuple[str, str, Optional[Dict], bool]:
        """Process a single Jenkins build.
        
        Args:
            context_url: Tuple of (build_context, jenkins_url)
            
        Returns:
            Tuple of (context, url, test_results, is_404)
        """
        context, url = context_url
        console_log, is_404 = self.download_console_log(url)
        if not console_log:
            return context, url, None, is_404
        
        test_results = self.parse_test_results(console_log)
        return context, url, test_results, False
    
    def _should_cancel_remaining_builds(self, not_found_count, processed_count,
                                       early_404_count, builds_processed):
        """Check if remaining builds should be cancelled due to 404s.
        
        Returns:
            bool: True if builds should be cancelled
        """
        # Optimized early exit: if first MIN_404_THRESHOLD builds are ALL 404s
        if (early_404_count >= self.MIN_404_THRESHOLD and
            builds_processed <= self.MIN_404_THRESHOLD):
            return True
        
        # Standard early exit: if we've processed at least MIN_404_THRESHOLD
        # builds and ALL are 404s
        if (not_found_count >= self.MIN_404_THRESHOLD and
            not_found_count == processed_count):
            return True
        
        return False

    def _process_build_result(self, future, result, builds_processed,
                             total_futures, tid):
        """Process a single build result and update counters.
        
        Returns:
            tuple: (is_404, should_continue)
        """
        build_start = time.perf_counter()
        context, url, test_results, is_404 = future.result()
        build_time = time.perf_counter() - build_start
        
        if is_404:
            self.log(
                f"Build 404 ({builds_processed}/{total_futures}): {context} "
                f"({build_time:.2f}s)",
                thread_id=tid
            )
            return True, True
        
        if test_results is None:
            self.log(
                f"Build failed ({builds_processed}/{total_futures}): {context} "
                f"({build_time:.2f}s)",
                thread_id=tid
            )
            return False, True
        
        result['jenkins_builds'][context] = {
            'url': url,
            'test_results': test_results
        }
        
        if test_results['found']:
            self.log(
                f"Build complete ({builds_processed}/{total_futures}): {context} - "
                f"{test_results['passed']} passed, {test_results['failed']} failed "
                f"({build_time:.2f}s)",
                thread_id=tid
            )
        else:
            filter_msg = f" (filter: {self.test_filter})" if self.test_filter else ""
            self.log(
                f"Build complete ({builds_processed}/{total_futures}): {context} - "
                f"No matching tests found{filter_msg} ({build_time:.2f}s)",
                thread_id=tid
            )
        
        return False, False

    def _handle_404_result(self, future, futures, initial_submission_futures,
                           not_found_count, builds_processed, result, tid):
        """Update 404 tracking and cancel remaining builds when appropriate.
        
        Returns:
            int: Updated 404 count
        """
        not_found_count += 1
        early_404_count = 1 if future in initial_submission_futures else 0
        processed_non_failures = len(result['jenkins_builds']) + not_found_count
        
        if self._should_cancel_remaining_builds(
            not_found_count, processed_non_failures,
            early_404_count, builds_processed
        ):
            cancelled_count = sum(1 for f in futures if f.cancel())
            self.log(
                f"Cancelled {cancelled_count} remaining builds "
                f"(all 404s detected)",
                thread_id=tid
            )
            return not_found_count, True
        
        return not_found_count, False

    def _process_pr_builds(self, jenkins_urls, result, tid):
        """Process Jenkins builds for a PR in parallel.
        
        Returns:
            int: Number of 404 build results
        """
        not_found_count = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._process_build, item)
                      for item in jenkins_urls.items()]
            initial_submission_futures = set(
                futures[:self.MIN_404_THRESHOLD]
                if len(futures) >= self.MIN_404_THRESHOLD
                else futures
            )
            builds_processed = 0

            for future in as_completed(futures):
                builds_processed += 1
                is_404, should_continue = self._process_build_result(
                    future, result, builds_processed, len(futures), tid
                )

                if is_404:
                    not_found_count, should_stop = self._handle_404_result(
                        future, futures, initial_submission_futures,
                        not_found_count, builds_processed, result, tid
                    )
                    if should_stop:
                        break

                if should_continue:
                    continue

        return not_found_count

    def analyze_pr(self, pr: Dict) -> Dict:
        """Analyze test results for a single PR with parallel processing.
        
        Args:
            pr: PR dictionary from GitHub API
            
        Returns:
            Dictionary containing PR metadata and Jenkins build results
        """
        start_time = time.perf_counter()
        pr_number = pr['number']
        tid = threading.get_ident()
        
        result = {
            'pr_number': pr_number,
            'title': pr['title'],
            'merged_at': pr.get('merged_at', 'unknown'),
            'jenkins_builds': {}
        }
        
        # Extract Jenkins URLs
        url_extract_start = time.perf_counter()
        jenkins_urls = self.extract_jenkins_urls(pr)
        url_extract_time = time.perf_counter() - url_extract_start
        
        if not jenkins_urls:
            total_time = time.perf_counter() - start_time
            self.log(f"No Jenkins builds found (total: {total_time:.2f}s)",
                    thread_id=tid)
            return result
        
        self.log(
            f"Found {len(jenkins_urls)} Jenkins builds "
            f"(URL extraction: {url_extract_time:.2f}s)",
            thread_id=tid
        )
        
        processing_start = time.perf_counter()
        not_found_count = self._process_pr_builds(jenkins_urls, result, tid)
        processing_time = time.perf_counter() - processing_start
        total_time = time.perf_counter() - start_time
        
        self.log(
            f"PR #{pr_number} analysis complete: "
            f"{len(result['jenkins_builds'])} successful builds, "
            f"{not_found_count} 404s (processing: {processing_time:.2f}s, "
            f"total: {total_time:.2f}s)",
            thread_id=tid
        )
        
        return result
    
    def _collect_test_statistics(self, results):
        """Collect test statistics from analysis results.
        
        Returns:
            tuple: (total_tests_run, total_passed, total_failed,
                   test_failures, test_suite_stats)
        """
        total_tests_run = 0
        total_passed = 0
        total_failed = 0
        test_failures = defaultdict(list)
        test_suite_stats = defaultdict(lambda: {'passed': 0, 'failed': 0})
        
        for result in results:
            pr_number = result['pr_number']
            pr_title = result['title']
            
            for context, build in result['jenkins_builds'].items():
                if not build['test_results']['found']:
                    continue
                
                total_tests_run += build['test_results']['total_tests']
                total_passed += build['test_results']['passed']
                total_failed += build['test_results']['failed']
                
                # Track per-suite statistics
                for suite in build['test_results']['test_suites']:
                    if (suite in build['test_results']['failed_tests'] or
                        any(suite in ft for ft in build['test_results']['failed_tests'])):
                        test_suite_stats[suite]['failed'] += 1
                    else:
                        test_suite_stats[suite]['passed'] += 1
                
                # Track which PR/build failed each test
                for test in build['test_results']['failed_tests']:
                    test_failures[test].append({
                        'pr_number': pr_number,
                        'pr_title': pr_title,
                        'build_context': context
                    })
        
        return (total_tests_run, total_passed, total_failed,
                test_failures, test_suite_stats)

    def _print_summary_statistics(self, results, total_tests_run,
                                  total_passed, total_failed):
        """Print summary statistics to console."""
        total_prs = len(results)
        prs_with_jenkins = sum(1 for r in results if r['jenkins_builds'])
        prs_with_tests = sum(
            1 for r in results
            if any(b['test_results']['found']
                  for b in r['jenkins_builds'].values())
        )
        
        filter_msg = (f" (filter: {self.test_filter})"
                     if self.test_filter else " (all tests)")
        
        print(f"\n{'='*80}")
        print("ANALYSIS SUMMARY")
        print(f"{'='*80}\n")
        print(f"Total PRs analyzed: {total_prs}")
        print(f"PRs with Jenkins builds: {prs_with_jenkins}")
        print(f"PRs with test results{filter_msg}: {prs_with_tests}")
        print(f"\nTest Statistics:")
        print(f"  Total test executions: {total_tests_run}")
        print(f"  Total passed: {total_passed}")
        print(f"  Total failed: {total_failed}")
        
        if total_tests_run > 0:
            pass_rate = (total_passed / total_tests_run) * 100
            print(f"  Pass rate: {pass_rate:.2f}%")

    def _print_suite_statistics(self, test_suite_stats):
        """Print per-suite success rates."""
        if not test_suite_stats:
            return
        
        print(f"\nTest Suite Success Rates:")
        sorted_suites = sorted(test_suite_stats.items(), key=lambda x: x[0])
        for suite, stats in sorted_suites:
            total = stats['passed'] + stats['failed']
            if total > 0:
                success_rate = (stats['passed'] / total) * 100
                print(f"  {suite}: {success_rate:.1f}% "
                      f"({stats['passed']}/{total})")

    def _print_failure_report(self, test_failures):
        """Print detailed failure report."""
        sorted_failures = sorted(test_failures.items(),
                                key=lambda x: len(x[1]), reverse=True)
        
        if not sorted_failures:
            return sorted_failures
        
        print(f"\n{'='*80}")
        print("FAILURE REPORT (sorted by failure count)")
        print(f"{'='*80}\n")
        
        for test_name, failures in sorted_failures:
            print(f"{test_name}: {len(failures)} failure(s)")
            for failure in failures:
                pr_num = failure['pr_number']
                pr_title = failure['pr_title'][:60]
                build_ctx = failure['build_context']
                print(f"\tPR #{pr_num} ({build_ctx}): {pr_title}")
            print()
        
        return sorted_failures

    def _save_json_report(self, output_file, results, total_tests_run,
                         total_passed, total_failed, test_suite_stats,
                         sorted_failures):
        """Save detailed JSON report to file."""
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        total_prs = len(results)
        prs_with_jenkins = sum(1 for r in results if r['jenkins_builds'])
        prs_with_tests = sum(
            1 for r in results
            if any(b['test_results']['found']
                  for b in r['jenkins_builds'].values())
        )
        
        report = {
            'generated_at': datetime.now().isoformat(),
            'test_filter': self.test_filter,
            'summary': {
                'total_prs': total_prs,
                'prs_with_jenkins': prs_with_jenkins,
                'prs_with_tests': prs_with_tests,
                'total_tests_run': total_tests_run,
                'total_passed': total_passed,
                'total_failed': total_failed,
                'pass_rate': ((total_passed / total_tests_run * 100)
                            if total_tests_run > 0 else 0)
            },
            'test_suite_stats': {
                suite: {
                    'passed': stats['passed'],
                    'failed': stats['failed'],
                    'success_rate': (
                        (stats['passed'] /
                         (stats['passed'] + stats['failed']) * 100)
                        if (stats['passed'] + stats['failed']) > 0
                        else 100.0
                    )
                }
                for suite, stats in test_suite_stats.items()
            },
            'test_failures': {test: failures
                            for test, failures in sorted_failures},
            'pr_results': results
        }
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\nDetailed results saved to: {output_file}")

    def generate_report(self, results: List[Dict], output_file: str):
        """Generate analysis report with failure breakdown by test."""
        # Collect statistics
        (total_tests_run, total_passed, total_failed,
         test_failures, test_suite_stats) = self._collect_test_statistics(results)
        
        # Print console reports
        self._print_summary_statistics(results, total_tests_run,
                                       total_passed, total_failed)
        self._print_suite_statistics(test_suite_stats)
        sorted_failures = self._print_failure_report(test_failures)
        
        # Save JSON report
        self._save_json_report(output_file, results, total_tests_run,
                              total_passed, total_failed, test_suite_stats,
                              sorted_failures)


def parse_arguments():
    """Parse and return command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Analyze unit test results across Ceph PRs on Jenkins',
        epilog='''
Examples:
  # Analyze all tests across 100 recent PRs
  %(prog)s --max-prs 100

  # Focus on mgr unit tests in reef branch
  %(prog)s --release reef --test-filter unittest_mgr --max-prs 50

  # High-performance analysis with custom output
  %(prog)s --workers 20 --download-workers 15 --output my_report.json
        ''',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--max-prs',
        type=int,
        default=100,
        help='Maximum number of PRs to analyze (default: 100)'
    )
    parser.add_argument(
        '--output',
        default='unittest_analysis.json',
        help='Output file for detailed results (default: unittest_analysis.json)'
    )
    parser.add_argument(
        '--github-token',
        help='GitHub personal access token (optional, for higher rate limits)'
    )
    parser.add_argument(
        '--release',
        default='main',
        help='Target release branch to analyze PRs for (default: main). Examples: main, reef, quincy, pacific'
    )
    parser.add_argument(
        '--test-filter',
        default=None,
        help='Filter tests by name pattern (e.g., "unittest_mgr"). If not specified, analyzes ALL tests.'
    )
    parser.add_argument(
        '--workers',
        type=int,
        default=None,
        help='Number of parallel workers for analyzing PRs (default: auto-calculated based on CPU cores)'
    )
    parser.add_argument(
        '--download-workers',
        type=int,
        default=None,
        help='Number of parallel workers per PR for downloading logs (default: auto-calculated)'
    )
    parser.add_argument(
        '--max-connections',
        type=int,
        default=3000,
        help='Maximum total concurrent connections (default: 3000, prevents overwhelming servers)'
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Reduce terminal output (only show important messages)'
    )
    parser.add_argument(
        '--log-file',
        help='Write detailed logs to file (useful with --quiet)'
    )
    return parser.parse_args()


def calculate_worker_counts(args):
    """Calculate optimal worker and download worker counts.
    
    Returns:
        tuple: (workers, download_workers, total_connections)
    """
    cpu_count = os.cpu_count() or 4
    
    if args.workers is None or args.download_workers is None:
        # Mathematical optimization:
        # Goal: Maximize throughput while respecting max_connections
        # Constraint: workers × download_workers ≤ max_connections
        # Strategy: Balance PR-level and download-level parallelism
        
        if args.workers is None and args.download_workers is None:
            # Optimal distribution for I/O-bound workload
            # Use sqrt distribution: workers ≈ sqrt(max_connections)
            optimal_workers = int(args.max_connections ** 0.5)
            optimal_downloads = args.max_connections // optimal_workers
            
            # Clamp to reasonable ranges
            workers = min(optimal_workers, cpu_count // 2, 100)
            download_workers = max(3, min(optimal_downloads, 10))
        elif args.workers is None:
            # User specified download_workers, calculate workers
            download_workers = args.download_workers
            workers = min(args.max_connections // download_workers, cpu_count // 2, 100)
        else:
            # User specified workers, calculate download_workers
            workers = args.workers
            download_workers = max(3, min(args.max_connections // workers, 10))
    else:
        workers = args.workers
        download_workers = args.download_workers
    
    total_connections = workers * download_workers
    return workers, download_workers, total_connections


def print_configuration(args, workers, download_workers, total_connections):
    """Print worker configuration and analysis settings."""
    cpu_count = os.cpu_count() or 4
    
    print(f"\n{'='*80}")
    print("WORKER CONFIGURATION")
    print(f"{'='*80}")
    print(f"CPU cores available: {cpu_count}")
    print(f"PR-level workers: {workers}")
    print(f"Download workers per PR: {download_workers}")
    print(f"Total concurrent connections: {total_connections}")
    print(f"Max connections limit: {args.max_connections}")
    print(f"{'='*80}\n")
    
    print(f"Target release: {args.release}")
    if args.test_filter:
        print(f"Test filter: {args.test_filter}")
    else:
        print("Test filter: None (analyzing ALL tests)")
    print()


def create_pr_chunks(prs, workers):
    """Divide PRs into chunks for parallel processing.
    
    Args:
        prs: List of PR dictionaries
        workers: Number of worker threads
        
    Returns:
        list: List of PR chunks, each containing (index, pr) tuples
    """
    pr_chunks = [[] for _ in range(workers)]
    for i, pr in enumerate(prs):
        pr_chunks[i % workers].append((i + 1, pr))
    
    # Remove empty chunks
    return [chunk for chunk in pr_chunks if chunk]


def analyze_pr_chunk(chunk, analyzer, prs, completed_count, progress_lock):
    """Analyze a pre-assigned chunk of PRs in a single thread.
    
    Args:
        chunk: List of (index, pr) tuples to analyze
        analyzer: JenkinsAnalyzer instance
        prs: Full list of PRs (for progress reporting)
        completed_count: Shared list with single counter element
        progress_lock: Threading lock for progress updates
        
    Returns:
        list: Analysis results for the chunk
    """
    thread_id = threading.get_ident()
    chunk_results = []
    
    for i, pr in chunk:
        result = analyzer.analyze_pr(pr)
        chunk_results.append(result)
        
        # Thread-safe progress reporting
        with progress_lock:
            completed_count[0] += 1
            print(f"[{completed_count[0]}/{len(prs)}] [TID:{thread_id}] "
                  f"Completed PR #{pr['number']}: {pr['title'][:50]}...")
    
    return chunk_results


def run_parallel_analysis(prs, pr_chunks, analyzer, download_workers):
    """Execute parallel analysis of PR chunks.
    
    Args:
        prs: Full list of PRs
        pr_chunks: List of PR chunks for parallel processing
        analyzer: JenkinsAnalyzer instance
        download_workers: Number of download workers per PR
        
    Returns:
        list: Combined analysis results from all chunks
    """
    print(f"Analyzing {len(prs)} PRs with {len(pr_chunks)} workers...")
    print(f"PRs per worker: {[len(chunk) for chunk in pr_chunks]}")
    print(f"Each worker uses {download_workers} download threads\n")
    
    results = []
    completed_count = [0]
    progress_lock = threading.Lock()
    
    with ThreadPoolExecutor(max_workers=len(pr_chunks)) as executor:
        # Submit chunk analysis tasks (one per worker)
        futures = [
            executor.submit(analyze_pr_chunk, chunk, analyzer, prs,
                          completed_count, progress_lock)
            for chunk in pr_chunks
        ]
        
        # Collect results as they complete
        for future in as_completed(futures):
            try:
                chunk_results = future.result()
                results.extend(chunk_results)
            except Exception as e:
                print(f"\nError analyzing PR chunk: {e}")
    
    return results


def main():
    """Main entry point for the Jenkins unit test analyzer."""
    args = parse_arguments()
    
    # Calculate worker configuration
    workers, download_workers, total_connections = calculate_worker_counts(args)
    
    # Print configuration
    print_configuration(args, workers, download_workers, total_connections)
    
    # Create analyzer
    analyzer = JenkinsAnalyzer(
        github_token=args.github_token,
        max_workers=download_workers,
        test_filter=args.test_filter
    )
    
    # Fetch PRs
    prs = analyzer.fetch_recent_prs(max_prs=args.max_prs, release=args.release)
    
    if not prs:
        print("No PRs found to analyze")
        return
    
    # Create PR chunks for parallel processing
    pr_chunks = create_pr_chunks(prs, workers)
    
    # Run parallel analysis
    results = run_parallel_analysis(prs, pr_chunks, analyzer, download_workers)
    
    # Generate report
    analyzer.generate_report(results, args.output)


if __name__ == '__main__':
    main()

# Made with Bob
