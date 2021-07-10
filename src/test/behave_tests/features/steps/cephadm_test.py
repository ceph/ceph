import logging

from behave import given, when, then
from kcli_handler import exec_ssh_cmd
from validation_util import stdoutput_to_list


@given("I log as root into {node} and I execute")
def init_step(context, node):
    context.node = node
    commands = context.text.split("\n")
    for command in commands:
        exec_ssh_cmd(context.node, command)


@when("I execute")
def execute_step(context):
    output, return_code = exec_ssh_cmd(context.node, context.text)
    if return_code == 0:
        context.output = stdoutput_to_list(output)
    logging.info(f"exec output : {context.output}")


@then("I get results which contain")
def validation_step(context):
    expected_keywords = context.text.split(" ")
    for key in expected_keywords:
        assert key in context.output, f"{key} not found\n{context.output}"
