import requests
from behave import given, when, then
from behave import *
from utils import *
import subprocess
import re

PROMETHEUS_SERVER_URL = None

def get_prometheus_pod_host_ip():
    try:
        command = "minikube --profile minikube kubectl -- -n rook-ceph -o jsonpath='{.status.hostIP}' get pod prometheus-rook-prometheus-0"
        result = subprocess.run(command, shell=True, capture_output=True, text=True, check=True)
        host_ip = result.stdout.strip()
        return host_ip
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {e}")
        return None

@when("I run ceph command")
def run_step(context):
    context.output = run_ceph_commands(context.text)

@when("I run k8s command")
def run_step(context):
    context.output = run_k8s_commands(context.text)

@then("I get")
def verify_result_step(context):
    if (context.text != context.output):
        display_side_by_side(context.text, context.output)
    assert context.text == context.output, ""

@then("I get something like")
def verify_fuzzy_result_step(context):
    output_lines = context.output.split("\n")
    expected_lines = context.text.split("\n")
    num_lines = min(len(output_lines), len(expected_lines))
    for n in range(num_lines):
        if not re.match(expected_lines[n], output_lines[n]):
            display_side_by_side(expected_lines[n], output_lines[n])
            assert False, ""

@given('I can get prometheus server configuration')
def step_get_prometheus_server_ip(context):
    global PROMETHEUS_SERVER_URL
    try:
        PROMETHEUS_SERVER_URL = f"http://{get_prometheus_pod_host_ip()}:30900"
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to Prometheus server: {e}")
        assert False, f"Error connecting to Prometheus server: {e}"

@given('the prometheus server is serving metrics')
def step_given_server_running(context):
    try:
        params = {'match[]': '{__name__!=""}'}
        response = requests.get(f"{PROMETHEUS_SERVER_URL}/federate", params)
        # Check if the response status code is successful (2xx)
        response.raise_for_status()
        # Store the response object in the context for later use
        context.response = response
        print(f"Prometheus server is running. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to Prometheus server: {e}")
        assert False, f"Error connecting to Prometheus server: {e}"

@when('I query the Prometheus metrics endpoint')
def step_when_query_metrics_endpoint(context):
    params = {'match[]': '{__name__!=""}'}
    context.response = requests.get(f"{PROMETHEUS_SERVER_URL}/federate", params)
    context.response.raise_for_status()

@then('the response contains the metric "{metric_name}"')
def step_then_check_metric_value(context, metric_name):
    metric_value = parse_metric_value(context.response.text, metric_name)
    assert metric_value is not None, f"Metric '{metric_name}' not found in the response"

@then('the response contains the metric "{metric_name}" with value equal to {expected_value}')
def step_then_check_metric_value(context, metric_name, expected_value):
    metric_value = parse_metric_value(context.response.text, metric_name)
    assert metric_value is not None, f"Metric '{metric_name}' not found in the response"
    assert metric_value == float(expected_value), f"Metric '{metric_name}' value {metric_value} is not equal to {expected_value}"

@then('the response contains the metric "{metric_name}" with value greater than {expected_value}')
def step_then_check_metric_value(context, metric_name, expected_value):
    metric_value = parse_metric_value(context.response.text, metric_name)
    assert metric_value is not None, f"Metric '{metric_name}' not found in the response"
    assert metric_value > float(expected_value), f"Metric '{metric_name}' value {metric_value} is not greater than {expected_value}"

@then('the response contains the metric "{metric_name}" with value less than {expected_value}')
def step_then_check_metric_value(context, metric_name, expected_value):
    metric_value = parse_metric_value(context.response.text, metric_name)
    assert metric_value is not None, f"Metric '{metric_name}' not found in the response"
    assert metric_value < float(expected_value), f"Metric '{metric_name}' value {metric_value} is not less than {expected_value}"

@then('the response contains the metric "{metric_name}" with value in the range {min_value}-{max_value}')
def step_then_check_metric_value(context, metric_name, min_value, max_value):
    metric_value = parse_metric_value(context.response.text, metric_name)
    assert metric_value is not None, f"Metric '{metric_name}' not found in the response"
    assert metric_value >= float(min_value) and metric_value <= float(max_value), f"Metric '{metric_name}' value {metric_value} is not in the range {min_value}-{max_value}"

@then('the response contains the metric "{metric_name}" where "{filter_by_field}" is "{field_value}" and value equal to {expected_value}')
def step_then_check_metric_value(context, metric_name, expected_value, filter_by_field, field_value):
    metric_value = parse_metric_value(context.response.text, metric_name, filter_by_field, field_value)
    assert metric_value is not None, f"Metric '{metric_name}' not found in the response"
    assert metric_value == float(expected_value), f"Metric '{metric_name}' value {metric_value} is not equal to {expected_value}"


def parse_metric_value(metrics_text, metric_name, filter_by_field=None, field_value=None):
    filter_condition = f'{filter_by_field}="{field_value}"' if filter_by_field and field_value else ''
    pattern_str = rf'^{metric_name}\{{[^}}]*{filter_condition}[^}}]*\}} (\d+) (\d+)'
    pattern = re.compile(pattern_str, re.MULTILINE)
    match = pattern.search(metrics_text)
    if match:
        # Extract the values and timestamp from the matched groups
        metric_value, _ = match.groups()
        return float(metric_value)
    else:
        # Metric not found
        return None
