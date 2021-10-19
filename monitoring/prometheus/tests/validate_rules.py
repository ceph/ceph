#!/usr/bin/python3 -u
#
# Check the Prometheus rules for format, and integration
# with the unit tests. This script has the following exit
# codes:
#  0 .. Everything worked
#  4 .. rule problems or missing unit tests
#  8 .. Missing fields in YAML
# 12 .. Invalid YAML - unable to load
# 16 .. Missing input files

# TODO: logging for debug

import re
import os
import sys
import yaml
from bs4 import BeautifulSoup
from typing import List, Any, Dict, Set, Optional, Tuple

import urllib.request
import urllib.error
from urllib.parse import urlparse

DOCLINK_NAME = 'documentation'
DEFAULT_RULES_FILENAME = '../alerts/ceph_default_alerts.yml'
DEFAULT_TEST_FILENAME = 'test_alerts.yml'


def read_file(file_name: str) -> Tuple[str, str]:
    try:
        with open(file_name, 'r') as input_file:
            raw_data = input_file.read()
    except OSError:
        return '', f"Unable to open {file_name}"

    return raw_data, ''


def load_yaml(file_name: str) -> Tuple[Dict[str, Any], str]:
    data = {}
    errs = ''

    raw_data, err = read_file(file_name)
    if not err:

        try:
            data = yaml.safe_load(raw_data)
        except yaml.YAMLError as e:
            errs = f"filename '{file_name} is not a valid YAML file"

    return data, errs


class HTMLCache:
    def __init__(self) -> None:
        self.cache: Dict[str, Tuple[int, str]] = {}

    def fetch(self, url_str: str) -> None:
        parsed = urlparse(url_str)
        url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

        if url in self.cache:
            return self.cache[url]

        req = urllib.request.Request(url)
        try:
            r = urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            self.cache[url] = e.code, e.reason
            return self.cache[url]
        except urllib.error.URLError as e:
            self.cache[url] = 400, e.reason
            return self.cache[url]

        if r.status == 200:
            html = r.read().decode('utf-8')
            self.cache[url] = 200, html
            return self.cache[url]

        self.cache[url] = r.status, r.reason
        return r.status, r.reason

    @property
    def cached_pages(self) -> List[str]:
        return self.cache.keys()

    @property
    def cached_pages_total(self) -> int:
        return len(self.cache.keys())

class PrometheusRule:
    expected_attrs = [
        'alert',
        'expr',
        'for',
        'labels',
        'annotations'
    ]

    def __init__(self, rule_group, rule_data: Dict[str, Any]):

        assert 'alert' in rule_data
        self.group: RuleGroup = rule_group
        self.name = rule_data.get('alert')
        self.rule = rule_data
        self.errors: List[str] = []
        self.warnings: List[str] = []

        self.validate()

    def _check_alert_name(self):
        pass

    def _check_structure(self):
        rule_attrs = self.rule.keys()
        missing_attrs = [a for a in PrometheusRule.expected_attrs if a not in rule_attrs]

        if missing_attrs:
            self.errors.append(
                f"invalid alert structure. Missing field{'s' if len(missing_attrs) > 1 else ''}"
                f": {','.join(missing_attrs)}")

    def _check_labels(self):
        for rqd in ['severity', 'type']:
            if rqd not in self.rule.get('labels', ''):
                self.errors.append(f"rule is missing {rqd} label definition")

    def _check_annotations(self):
        for rqd in ['description']:
                if rqd not in self.rule.get('annotations', ''):
                    self.errors.append(f"rule is missing {rqd} annotation definition")

    def _check_doclink(self):
        annotations = self.rule.get('annotations', {})
        doclink = annotations.get(DOCLINK_NAME, '')

        if doclink:
            url = urlparse(doclink)
            status, content = self.group.fetch_html_page(doclink)
            if status == 200:
                if url.fragment:
                    soup = BeautifulSoup(content, 'html.parser')
                    if not soup.find(id=url.fragment):
                        self.errors.append(f"documentation link error: {url.fragment} anchor not found on the page")
            else:
                # catch all
                self.errors.append(f"documentation link error: {status} {content}")

    def _check_snmp(self):
        labels = self.rule.get('labels', {})
        oid = labels.get('oid', '')
        if labels.get('severity', '') == 'critical' and not oid:
            self.warnings.append("critical level alert is missing an SNMP oid entry")
        if oid and not re.search('^1.3.6.1.4.1.50495.15.1.2.\\d+.\\d+$', oid):
            self.errors.append("invalid OID provided")

    def validate(self):
        self._check_alert_name()
        self._check_structure()
        self._check_labels()
        self._check_annotations()
        self._check_doclink()
        self._check_snmp()
        char = '.'

        if self.errors:
            char = 'E'
            self.group.update('error', self.name)
        elif self.warnings:
            char = 'W'
            self.group.update('warning', self.name)

        sys.stdout.write(char)


class RuleGroup:

    def __init__(self, rule_file, group_name: str, group_name_width: int):
        self.rule_file: RuleFile = rule_file
        self.group_name = group_name
        self.rules: Dict[str, PrometheusRule] = {}
        self.problems = {
            "error": [],
            "warning": [],
        }

        sys.stdout.write(f"\n\t{group_name:<{group_name_width}} : ")

    def add_rule(self, rule_data:Dict[str, Any]):
        alert_name = rule_data.get('alert')
        self.rules[alert_name] = PrometheusRule(self, rule_data)

    def update(self, problem_type:str, alert_name:str):
        assert problem_type in ['error', 'warning']

        self.problems[problem_type].append(alert_name)
        self.rule_file.update(self.group_name)

    def fetch_html_page(self, url):
        return self.rule_file.fetch_html_page(url)

    @property
    def error_count(self):
        return len(self.problems['error'])

    def warning_count(self):
        return len(self.problems['warning'])

    @property
    def count(self):
        return len(self.rules)


class RuleFile:

    def __init__(self, parent, file_name, rules):
        self.parent = parent
        self.file_name = file_name
        self.rules: Dict[str, Any] = rules
        self.problems: Set[str] = set()
        self.group: Dict[str, RuleGroup] = {}
        self.alert_names_seen: Set[str] = set()
        self.duplicate_alert_names:List[str] = []
        self.html_cache = HTMLCache()

        assert 'groups' in self.rules
        self.max_group_name_width = self.get_max_group_name()
        self.load_groups()

    def update(self, group_name):
        self.problems.add(group_name)
        self.parent.mark_invalid()

    def fetch_html_page(self, url):
        return self.html_cache.fetch(url)

    @property
    def group_count(self):
        return len(self.rules['groups'])

    @property
    def rule_count(self):
        rule_count = 0
        for group_name, rule_group in self.group.items():
            rule_count += rule_group.count
        return rule_count

    @property
    def group_names(self):
        return self.group.keys()

    @property
    def problem_count(self):
        return len(self.problems)

    def get_max_group_name(self):
        group_name_list = []
        for group in self.rules.get('groups'):
            group_name_list.append(group['name'])
        return max([len(g) for g in group_name_list])

    def load_groups(self):
        sys.stdout.write("\nChecking rule groups")
        for group in self.rules.get('groups'):
            group_name = group['name']
            rules = group['rules']
            self.group[group_name] = RuleGroup(self, group_name, self.max_group_name_width)
            for rule_data in rules:
                if 'alert' in rule_data:
                    alert_name = rule_data.get('alert')
                    if alert_name in self.alert_names_seen:
                        self.duplicate_alert_names.append(alert_name)
                    else:
                        self.alert_names_seen.add(alert_name)
                    self.group[group_name].add_rule(rule_data)
                else:
                    # skipped recording rule
                    pass

    def error_report(self):
        def max_width(item_list: List[str]) -> int:
            return max([len(i) for i in item_list])

        if not self.problems and not self.duplicate_alert_names:
            print("\nNo problems detected in rule file")
            return

        print("\nProblem Report\n")

        group_width = max_width(self.problems)
        alert_names = set()
        for g in self.problems:
            group = self.group[g]
            alert_names.update(group.problems.get('error', []))
            alert_names.update(group.problems.get('warning', []))
        alert_width = max_width(alert_names)

        template = "  {group:<{group_width}}  {severity:<8}  {alert_name:<{alert_width}}  {description}"

        print(template.format(
            group="Group",
            group_width=group_width,
            severity="Severity",
            alert_name="Alert Name",
            alert_width=alert_width,
            description="Problem Description"))

        print(template.format(
            group="-----",
            group_width=group_width,
            severity="--------",
            alert_name="----------",
            alert_width=alert_width,
            description="-------------------"))

        for group_name in sorted(self.problems):
            group = self.group[group_name]
            rules = group.rules
            for alert_name in group.problems.get('error', []):
                for desc in rules[alert_name].errors:
                    print(template.format(
                            group=group_name,
                            group_width=group_width,
                            severity="Error",
                            alert_name=alert_name,
                            alert_width=alert_width,
                            description=desc))
            for alert_name in group.problems.get('warning', []):
                for desc in rules[alert_name].warnings:
                    print(template.format(
                            group=group_name,
                            group_width=group_width,
                            severity="Warning",
                            alert_name=alert_name,
                            alert_width=alert_width,
                            description=desc))
        if self.duplicate_alert_names:
            print("Duplicate alert names detected:")
            for a in self.duplicate_alert_names:
                print(f"  - {a}")


class UnitTests:
    expected_attrs = [
        'rule_files',
        'tests',
        'evaluation_interval'
    ]
    def __init__(self, filename):
        self.filename = filename
        self.unit_test_data: Dict[str, Any] = {}
        self.alert_names_seen: Set[str] = set()
        self.problems: List[str] = []
        self.load()

    def load(self):
        self.unit_test_data, errs = load_yaml(self.filename)
        if errs:
            print(f"\n\nError in unit tests file: {errs}")
            sys.exit(12)

        missing_attr = [a for a in UnitTests.expected_attrs if a not in self.unit_test_data.keys()]
        if missing_attr:
            print(f"\nMissing attributes in unit tests: {','.join(missing_attr)}")
            sys.exit(8)

    def _check_alert_names(self, alert_names: List[str]):
        alerts_tested: Set[str] = set()
        for t in self.unit_test_data.get('tests'):
            test_cases = t.get('alert_rule_test', [])
            if not test_cases:
                continue
            for case in test_cases:
                alertname = case.get('alertname', '')
                if alertname:
                    alerts_tested.add(alertname)

        alerts_defined = set(alert_names)
        self.problems = list(alerts_defined.difference(alerts_tested))

    def process(self, defined_alert_names: List[str]):
        self._check_alert_names(defined_alert_names)

    def error_report(self) -> None:

        if not self.problems:
            print("\nNo problems detected in unit tests file")
            return

        print("\nUnit tests are incomplete. Tests missing for the following alerts;")
        for p in self.problems:
            print(f"  - {p}")

class RuleChecker:

    def __init__(self, rules_filename: str = None, test_filename: str = None):
        self.rules_filename = rules_filename or DEFAULT_RULES_FILENAME
        self.test_filename = test_filename or DEFAULT_TEST_FILENAME
        self.rule_file: Optional[RuleFile] = None
        self.unit_tests: Optional[UnitTests] = None
        self.rule_file_problems: bool = False
        self.errors = {}
        self.warnings = {}
        self.error_count = 0
        self.warning_count = 0

    @property
    def status(self):
        if self.rule_file_problems or self.unit_tests.problems:
            return 4

        return 0

    def mark_invalid(self):
        self.rule_file_problems = True

    def summarise_rule_file(self):
        for group_name in self.rule_file.problems:
            group = self.rule_file.group[group_name]
            self.error_count += len(group.problems['error'])
            self.warning_count += len(group.problems['warning'])

    def ready(self):
        errs: List[str] = []
        ready_state = True
        if not os.path.exists(self.rules_filename):
            errs.append(f"rule file '{self.rules_filename}' not found")
            ready_state = False

        if not os.path.exists(self.test_filename):
            errs.append(f"test file '{self.test_filename}' not found")
            ready_state = False

        return ready_state, errs

    def run(self):

        ready, errs = self.ready()
        if not ready:
            print("Unable to start:")
            for e in errs:
                print(f"- {e}")
            sys.exit(16)

        rules, errs = load_yaml(self.rules_filename)
        if errs:
            print(errs)
            sys.exit(12)

        self.rule_file = RuleFile(self, self.rules_filename, rules)
        self.summarise_rule_file()

        self.unit_tests = UnitTests(self.test_filename)
        self.unit_tests.process(self.rule_file.alert_names_seen)

    def error_report(self):
        print("\n\nSummary\n")
        print(f"Rule file             : {self.rules_filename}")
        print(f"Unit Test file        : {self.test_filename}")
        print(f"\nRule groups processed : {self.rule_file.group_count:>3}")
        print(f"Rules processed       : {self.rule_file.rule_count:>3}")
        print(f"Rule errors           : {self.error_count:>3}")
        print(f"Rule warnings         : {self.warning_count:>3}")
        print(f"Rule name duplicates  : {len(self.rule_file.duplicate_alert_names):>3}")
        print(f"Unit tests missing    : {len(self.unit_tests.problems):>3}")

        if self.rule_file_problems:
            self.rule_file.error_report()
        if self.unit_tests.problems:
            self.unit_tests.error_report()


def main():
    checker = RuleChecker()

    checker.run()
    if checker.status > 0:
        checker.error_report()
        print()

    sys.exit(checker.status)


if __name__ == '__main__':
    main()
