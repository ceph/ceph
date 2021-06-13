from behave import given, when, then


@given('I log as root into ceph-node-00 and I execute')
def init_step(context):
    pass


@when('I execute')
def execute_step(context):
    assert True is not False


@then('I get')
def validation_step(context):
    assert context.failed is False
