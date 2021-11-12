import time

from behave import given, when, then
from kcli_handler import execute_ssh_cmd
from validation_util import str_to_list


@given("I log as root into {node}")
def login_to_node(context, node):
    context.node = node


@given("I execute in {shell}")
def init_step_execute(context, shell):
    commands = context.text.split("\n")
    for command in commands:
        op, code = execute_ssh_cmd(context.node, shell, command)
        if code:
            raise Exception("Failed to execute")
        context.last_executed["cmd"] = command
        context.last_executed["shell"] = shell


@when("I execute in {shell}")
@then("I execute in {shell}")
def execute_step(context, shell):
    if context.node is None:
        raise Exception("Failed not logged into virtual machine")
    for command in context.text.split("\n"):
        output, return_code = execute_ssh_cmd(context.node, shell, command)
        context.last_executed["cmd"] = command
        context.last_executed["shell"] = shell
        if return_code != 0:
            raise Exception(f"Failed to execute ssh\n Message:{output}")
        context.output = str_to_list(output)
    print(f"Executed output : {context.output}")


@then("Execute in {shell} only {command}")
def execute_only_one_step(context, shell, command):
    """
    Run's single command and doesn't use multi-line
    :params command: given command to execute
    """
    if context.node is None:
        raise Exception("Failed not logged into virtual machine")
    output, return_code = execute_ssh_cmd(context.node, shell, command)
    context.last_executed["cmd"] = command
    context.last_executed["shell"] = shell
    if return_code != 0:
        raise Exception(f"Failed to execute ssh\nMessage:{output}")
    context.output = str_to_list(output)
    print(f"Executed output : {context.output}")


@then("I wait for {time_out:n} seconds until I get")
def execute_and_wait_until_step(context, time_out):
    wait_time = int(time_out/4)
    context.found_all_keywords = False
    if context.node is None:
        raise Exception("Failed not logged into virtual machine")
    exec_shell = context.last_executed['shell']
    exec_cmd = context.last_executed['cmd']
    if exec_shell is None and exec_cmd is None:
        raise Exception("Last executed command not found..")

    expected_output = str_to_list(context.text)
    while wait_time < time_out and not context.found_all_keywords:
        found_keys = []
        context.execute_steps(
            f"then Execute in {exec_shell} only {exec_cmd}"
        )

        executed_output = context.output
        for expected_line in expected_output:
            for op_line in executed_output:
                if set(expected_line).issubset(set(op_line)):
                    found_keys.append(" ".join(expected_line))

        if len(found_keys) != len(expected_output):
            print(f"Waiting for {int(time_out/4)} seconds")
            time.sleep(int(time_out/4))
            wait_time += int(time_out/4)
        else:
            print("Found all expected keywords")
            context.found_all_keywords = True
            break
    if not context.found_all_keywords:
        print(
            f"Timeout reached {time_out}. Giving up on waiting for keywords"
        )


@then("I get results which contain")
def validation_step(context):
    expected_keywords = str_to_list(context.text)
    output_lines = context.output

    for keys_line in expected_keywords:
        found_keyword = False
        for op_line in output_lines:
            if set(keys_line).issubset(set(op_line)):
                found_keyword = True
                output_lines.remove(op_line)
        if not found_keyword:
            assert False, f"Not found {keys_line}"
