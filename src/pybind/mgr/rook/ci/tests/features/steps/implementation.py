from behave import *
from utils import *
import re

@when("I run")
def run_step(context):
    context.output = run_commands(context.text)

@then("I get")
def verify_result_step(context):
    print(f"Output is:\n{context.output}\n--------------\n")
    assert context.text == context.output

@then("I get something like")
def verify_fuzzy_result_step(context):
    output_lines = context.output.split("\n")
    expected_lines = context.text.split("\n")
    num_lines = min(len(output_lines), len(expected_lines))
    for n in range(num_lines):
        if not re.match(expected_lines[n], output_lines[n]):
            raise
