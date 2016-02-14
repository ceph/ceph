#!/usr/bin/env python
#
# Copyright (C) 2016 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
from git import Repo  # GitPython
import argparse
import calendar
import github  # githubpy
import json
import logging
import os
import pprint
import requests_cache
import sys
import time

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')

log = logging.getLogger(__name__)

class CephFSReport(object):

    TIME_TEMPLATE = '%Y-%m-%dT%H:%M:%SZ'
    ONE_DAY = (24*60*60)

    def __init__(self):
        self.prs = {}

    def parser(self):
        parser = argparse.ArgumentParser(
            'ceph-fs-report',
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            default=None,
        )
        parser.add_argument(
            "--cache",
            default='cache',
            help="GitHub info cache")
        parser.add_argument(
            "--token", 
            default=os.getenv("GITHUB_ACCESS_TOKEN"),
            help="Github Access Token ($GITHUB_ACCESS_TOKEN otherwise)")
        parser.add_argument(
            "--repo",
            required=True,
            help="path to ceph git repo")
        return parser

    def run(self, argv):
        self.args = self.parser().parse_args(argv)

        if self.args.verbose:
            level = logging.DEBUG
        else:
            level = logging.INFO
        logging.getLogger('ceph_fs_report').setLevel(level)

        return self.main()

    def set_git(self):
        self.git = Repo(self.args.repo)

    def set_github(self):
        self.github = github.GitHub(access_token=self.args.token)
        self.github.rate_limit().get()
        self.repo = self.github.repos("ceph")("ceph")
        
    def is_devel(self, branch, created_at):
        if branch in ('master', 'next'):
            return True
        releases = {
            'jewel': time.strptime('3000-01-01', '%Y-%m-%d'),
            'infernalis': time.strptime('2015-11-06', '%Y-%m-%d'),
            'hammer': time.strptime('2015-04-07', '%Y-%m-%d'),
        }
        if branch not in releases:
            return False
        created_at = time.strptime(created_at, self.TIME_TEMPLATE)
        return created_at < releases[branch]

    @staticmethod
    def pr_p(number):
        return 'https://github.com/ceph/ceph/pull/' + str(number)
            
    def is_selected_labels(self, labels, number):
        components = set(['cephfs', 'core', 'common', 'documentation',
                          'pybind', 'rbd', 'rgw', 'tests', 'tools',
                          'build/ops', 'trash'])
        selected_components = components - set(['cephfs', 'rgw', 'trash'])
        labels = set([l['name'] for l in labels])
        # PR 2500 was created 15 sept 2014, labeling was not done before that date
        if not components.intersection(labels) and number > 2500:
            log.error(self.pr_p(number) + ": missing component " + 
                      str(components))
        return labels.intersection(selected_components)

    def process_pr(self, pr):
        base = pr['base'].get('ref')
        if not self.is_devel(base, pr['created_at']):
            log.debug("SKIP " + str(pr['number']) + " because " +
                      base + " was not a development branch on the " +
                      pr['created_at'])
            return "SKIP not dev"
        issue = self.issues[pr['number']]
        if not self.is_selected_labels(issue['labels'], pr['number']):
            log.debug("skip " + str(pr['number']) + " because some labels (" +
                      str(issue['labels']) + ") are excluded or missing")
            return "SKIP not selected"
        log.debug("loaded pr " + str(pr['number']))
        return "OK"

    def get_prs_page(self, page, per_page):
        return self.repo.pulls().get(state='all', page=page, per_page=per_page)
        
    def get_issues_page(self, page, per_page):
        return self.repo.issues().get(state='all', page=page, per_page=per_page)
        
    def get_comments_page(self, page, per_page):
        return self.repo.issues().comments().get(page=page, per_page=per_page)

    def get_all_pages(self, get_page):
        results = []
        per_page = 100
        page = 1
        while True:
            results_page = get_page(page=page, per_page=per_page)
            results.extend(results_page)
            if len(results_page) == 0:
                break
            page += 1
        return results
        
    def get_all(self, name, get_page):
        cache_path = self.args.cache + '-' + name + '.json'
        if os.path.exists(cache_path):
            return json.load(open(cache_path, 'r'))
        results = self.get_all_pages(get_page)
        json.dump(results, open(cache_path, 'w'))
        return results

    def get_issues(self):
        self.issues = {}
        for issue in self.get_all('issues', self.get_issues_page):
            self.issues[issue['number']] = issue

    def get_detailed_prs(self):
        name = 'detailed-prs'
        cache_path = self.args.cache + '-' + name + '.json'
        if os.path.exists(cache_path):
            self.detailed_prs = json.load(open(cache_path, 'r'))
            return
        prs = []
        for pr in self.prs:
            if self.process_pr(pr) == 'OK':
                prs.append(pr)
        results = []
        for pr in prs:
            log.info("loading detailed PR " + str(pr['number']))
            results.append(self.repo.pulls(pr['number']).get())
        json.dump(results, open(cache_path, 'w'))
        self.detailed_prs = results
        
    def get_events(self):
        name = 'events'
        cache_path = self.args.cache + '-' + name + '.json'
        if os.path.exists(cache_path):
            self.events = json.load(open(cache_path, 'r'))
            return
        prs = []
        for pr in self.prs:
            if self.process_pr(pr) == 'OK':
                prs.append(pr)
        results = {}
        for pr in prs:
            self.throttle()
            log.info("loading events for PR " + str(pr['number']))
            def get_events_page(page, per_page):
                log.info("loading events page " + str(pr['number']) + " " + str(page) + " " + str(per_page))
                return self.repo.issues(pr['number']).events().get(page=page, per_page=per_page)
            events = self.get_all_pages(get_events_page)
            detailed_events = []
            for event in events:
                event_number = event['url'].split('/')[-1]
                log.info("loading event " + event_number)
                detailed_events.append(self.repo.issues().events(int(event_number)).get())
            results[pr['number']] = detailed_events
        json.dump(results, open(cache_path, 'w'))
        self.events = results

    def throttle(self):
        if self.github.x_ratelimit_remaining < 1000:
            pause = self.github.x_ratelimit_reset - time.time()
            log.info("waiting for ratelimit reset " + str(pause))
            time.sleep(pause)

    def get_prs(self):
        self.prs = self.get_all('prs', self.get_prs_page)

    def get_comments(self):
        self.comments = self.get_all('comments', self.get_comments_page)

    def ages_histogram(self, prs):
        ages = {}
        for pr in prs:
            created_at = time.strptime(pr['created_at'],
                                       self.TIME_TEMPLATE)
            created_at = calendar.timegm(created_at)
            active_until = None
            if pr['merged_at']:
                active_until = pr['merged_at']
            elif pr['closed_at']:
                active_until = pr['closed_at']
            if active_until:
                active_until = calendar.timegm(
                    time.strptime(active_until,self.TIME_TEMPLATE))
            else:
                active_until = time.time()
            current = created_at
            age = 1
            while current <= active_until:
                day = current / self.ONE_DAY
                existing = ages.setdefault(day, {}).get(age, 0)
                ages[day][age] = existing + 1
                current += self.ONE_DAY
                age += 1
        return ages
        
    def print_ages(self):
        prs = []
        for pr in self.prs:
            if self.process_pr(pr) == 'OK':
                prs.append(pr)
        ages = self.ages_histogram(prs)
        average_age = []
        for day in sorted(ages.keys()):
            info = ages[day]
            log.debug(str(day) + " " + str(info))
            total_pr_count = sum(info.values())
            average = 0
            for (age, pr_count) in info.iteritems():
                average += age * (float(pr_count) / total_pr_count)
            average_age.append([day * self.ONE_DAY * 1000, int(average)])
        print("ages_over_time = " + str(average_age))

    def print_closers(self):
        prs = []
        for pr in self.detailed_prs:
            if self.process_pr(pr) == 'OK':
                prs.append(pr)
        closers = {}
        for pr in prs:
            issue = self.issues[pr['number']]
            if issue['closed_at']:
                closed_at = calendar.timegm(
                    time.strptime(issue['closed_at'],self.TIME_TEMPLATE))
                day = closed_at / self.ONE_DAY
#                log.info("pr " + str(pr))
#                log.info("issue " + str(issue))
                if 'closed_by' in issue and issue['closed_by']:
                    who = issue['closed_by']['login']
                elif 'merged_by' in pr and pr['merged_by']:
                    who = pr['merged_by']['login']
                else:
                    log.error('missing closed_by and merged_by in ' + str(pr['number']))
                    continue
                if who != 'liewegas':
                    who = 'others'
                count = closers.setdefault(who, {}).get(day, 0)
                closers[who][day] = count + 1
        datas = []
        for (who, histogram) in closers.iteritems():
            data = []
            for (day, count) in histogram.iteritems():
                data.append([day * self.ONE_DAY * 1000, count])
            datas.append('{ data: ' + str(sorted(data)) + ', label: "' + who + '"}')
        print('closers = [' + ','.join(datas) + '];')
        
    def main(self):
        self.set_github()
        self.set_git()
        self.get_prs()
        self.get_issues()
        self.get_detailed_prs()
#        self.get_comments()
        self.get_events()
        self.print_ages()
        self.print_closers()

def run():
    CephFSReport().run(sys.argv[1:])
