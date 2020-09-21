# -*- coding: utf-8 -*-
# pylint: disable=F0401
"""
This script does:
* Scan through Angular html templates and extract <cd-grafana> tags
* Check if every tag has a corresponding Grafana dashboard by `uid`

Usage:
    python <script> <angular_app_dir> <grafana_dashboard_dir>

e.g.
    cd /ceph/src/pybind/mgr/dashboard
    python ci/<script> frontend/src/app /ceph/monitoring/grafana/dashboards
"""
import argparse
import codecs
import copy
import json
import os

from html.parser import HTMLParser


class TemplateParser(HTMLParser):

    def __init__(self, _file, search_tag):
        super().__init__()
        self.search_tag = search_tag
        self.file = _file
        self.parsed_data = []

    def parse(self):
        with codecs.open(self.file, encoding='UTF-8') as f:
            self.feed(f.read())

    def handle_starttag(self, tag, attrs):
        if tag != self.search_tag:
            return
        tag_data = {
            'file': self.file,
            'attrs': dict(attrs),
            'line': self.getpos()[0]
        }
        self.parsed_data.append(tag_data)

    def error(self, message):
        error_msg = 'fail to parse file {} (@{}): {}'.\
            format(self.file, self.getpos(), message)
        exit(error_msg)


def get_files(base_dir, file_ext):
    result = []
    for root, _, files in os.walk(base_dir):
        for _file in files:
            if _file.endswith('.{}'.format(file_ext)):
                result.append(os.path.join(root, _file))
    return result


def get_tags(base_dir, tag='cd-grafana'):
    templates = get_files(base_dir, 'html')
    tags = []
    for templ in templates:
        parser = TemplateParser(templ, tag)
        parser.parse()
        if parser.parsed_data:
            tags.extend(parser.parsed_data)
    return tags


def get_grafana_dashboards(base_dir):
    json_files = get_files(base_dir, 'json')
    dashboards = {}
    for json_file in json_files:
        with open(json_file) as f:
            dashboard_config = json.load(f)
            uid = dashboard_config.get('uid')

            # Grafana dashboard checks
            title = dashboard_config['title']
            assert len(title) > 0, \
                "Title not found in '{}'".format(json_file)
            assert len(dashboard_config.get('links', [])) == 0, \
                "Links found in '{}'".format(json_file)
            if not uid:
                continue
            if uid in dashboards:
                # duplicated uids
                error_msg = 'Duplicated UID {} found, already defined in {}'.\
                    format(uid, dashboards[uid]['file'])
                exit(error_msg)

            dashboards[uid] = {
                'file': json_file,
                'title': title
            }
    return dashboards


def parse_args():
    long_desc = ('Check every <cd-grafana> component in Angular template has a'
                 ' mapped Grafana dashboard.')
    parser = argparse.ArgumentParser(description=long_desc)
    parser.add_argument('angular_app_dir', type=str,
                        help='Angular app base directory')
    parser.add_argument('grafana_dash_dir', type=str,
                        help='Directory contains Grafana dashboard JSON files')
    parser.add_argument('--verbose', action='store_true',
                        help='Display verbose mapping information.')
    return parser.parse_args()


def main():
    args = parse_args()
    tags = get_tags(args.angular_app_dir)
    grafana_dashboards = get_grafana_dashboards(args.grafana_dash_dir)
    verbose = args.verbose

    if not tags:
        error_msg = 'Can not find any cd-grafana component under {}'.\
            format(args.angular_app_dir)
        exit(error_msg)

    if verbose:
        print('Found mappings:')
    no_dashboard_tags = []
    for tag in tags:
        uid = tag['attrs']['uid']
        if uid not in grafana_dashboards:
            no_dashboard_tags.append(copy.copy(tag))
            continue
        if verbose:
            msg = '{} ({}:{}) \n\t-> {} ({})'.\
                format(uid, tag['file'], tag['line'],
                       grafana_dashboards[uid]['title'],
                       grafana_dashboards[uid]['file'])
            print(msg)

    if no_dashboard_tags:
        title = ('Checking Grafana dashboards UIDs: ERROR\n'
                 'Components that have no mapped Grafana dashboards:\n')
        lines = ('{} ({}:{})'.format(tag['attrs']['uid'],
                                     tag['file'],
                                     tag['line'])
                 for tag in no_dashboard_tags)
        error_msg = title + '\n'.join(lines)
        exit(error_msg)
    else:
        print('Checking Grafana dashboards UIDs: OK')


if __name__ == '__main__':
    main()
