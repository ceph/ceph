from copy import deepcopy
from rest.report import Report

report_data = {
    "crashes": {
        "crashid1": {"crash_id": "crashreport1"},
        "crashid2": {
            "os_name": "TheBestOS",
            "utsname_hostname": "foo.bar.baz.com",
        },
    },
    "key.with.dots": "value.with.dots.and.%",
    "key.with.dots.and.%": "value.with.dots.and.%",
    "key1": {
        "key2": {
            "key3.with.dots": "value3",
        },
    },
    "report_timestamp": "2019-04-25T22:42:59.083915",
    "report_id": "cc74d980-51ba-4c29-8534-fa813e759a7c",
}



def test_dots_to_percent():
    report = Report(report_data)
    report._dots_to_percent()
    assert('key.with.dots' not in report.report)
    assert('key%with%dots' in report.report)
    assert('key%with%dots%and%%%' in report.report)
    assert(report.report['key%with%dots'] == 'value.with.dots.and.%')
    assert('key3%with%dots' in report.report['key1']['key2'])


def test_crashes_to_list():
    report = Report(report_data)
    report._crashes_to_list()
    assert(isinstance(report.report['crashes'], list))
    assert(len(report.report['crashes']) == 2)
    assert({'crash_id' : 'crashreport1'} in report.report['crashes'])
    assert({"os_name": "TheBestOS", "utsname_hostname": "foo.bar.baz.com"} in report.report['crashes'])


def test_report_id():
    report = Report(report_data)
    assert(report._report_id() ==
           'cc74d980-51ba-4c29-8534-fa813e759a7c.2019-04-25T22:42:59.083915')
    del report.report['report_timestamp']
    es_id = report._report_id()
    assert(es_id.startswith('cc74d980-51ba-4c29-8534-fa813e759a7c'))


def test_purge_hostname_from_crash():
    report = Report(report_data)
    report._purge_hostname_from_crash()
    assert({"os_name": "TheBestOS", "utsname_hostname": "foo.bar.baz.com"} not in report.report['crashes'])
    assert({"os_name": "TheBestOS"} in report.report['crashes'])
