import time

from behave import given, when, then  # type: ignore
from kcli_handler import execute_ssh_cmd
from validation_util import missing_tokens


def exec_cmds(context, shell, cmds):
    context.output = ''
    if context.node is None:
        raise Exception("Failed not logged into virtual machine")
    for command in cmds:
        out, err, code = execute_ssh_cmd(context, context.node, shell, command)
        context.last_executed["cmd"] = command
        context.last_executed["shell"] = shell
        if code != 0:
            raise Exception(f"Failed to execute ssh\nOut:{out}\nErr:{err}")
        context.output += out + '\n' + err + '\n'


@given("I log as root into {node}")
def login_to_node(context, node):
    context.node = node


@given("I execute in {shell}")
@when("I execute in {shell}")
@then("I execute in {shell}")
def execute_step(context, shell):
    exec_cmds(context, shell, context.text.split('\n'))


@then("I wait for {time_out:n} seconds until I get")
def execute_and_wait_until_step(context, time_out):
    interval = int(time_out/8)
    wait_time = 0
    exec_shell = context.last_executed['shell'] if context.last_executed['shell'] else ''
    exec_cmd = context.last_executed['cmd']
    if exec_cmd is None:
        raise Exception("No previously executed command to wait for!")

    missing_output = []
    while wait_time < time_out:
        exec_cmds(context, exec_shell, [exec_cmd])
        missing_output = missing_tokens(context.text, context.output)

        if missing_output:
            time.sleep(interval)
            wait_time += interval
        else:
            return
    raise Exception(f'Missing line(s) of keywords ({missing_output}) in output:\n{context.output}')


@then("I get results which contain")
def validation_step(context):
    missing_output = missing_tokens(context.text, context.output)

    if missing_output:
        raise Exception(f'Missing line(s) of keywords ({missing_output}) in output:\n{context.output}')
