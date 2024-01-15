from behave import *
from utils import *
import re

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
