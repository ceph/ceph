from behave import *
from kcli_handler import exec_ssh_cmd


@given('I log as root into {node} and I execute')
def init_step(context, node):
    context.node = node
    commands = context.text.split('\n')
    for command in commands:
        exec_ssh_cmd(context.node, command)

@when('I execute')
def execute_step(context):
    context.output = exec_ssh_cmd(
        context.node,
        context.text
    )

@then('I get')
def validation_step(context):
    assert context.text == context.output
