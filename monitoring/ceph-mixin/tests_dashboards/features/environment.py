# type: ignore[no-redef]
# pylint: disable=E0611,W0613,E0102
import copy

from behave import given, then, when
from prettytable import PrettyTable

from tests_dashboards import PromqlTest
from tests_dashboards.util import get_dashboards_data, resolve_time_and_unit


class GlobalContext:
    def __init__(self):
        self.tested_queries_count = 0
        self.promql_expr_test = None
        self.data = get_dashboards_data()
        self.query_map = self.data['queries']

    def reset_promql_test(self):
        self.promql_expr_test = PromqlTest()
        self.promql_expr_test.variables = copy.copy(self.data['variables'])

    def print_query_stats(self):
        total = len(self.query_map)
        table = PrettyTable()
        table.field_names = ['Name', 'Queries', 'Tested', 'Cover']

        def percent(tested, total):
            return str(round((tested / total) * 100, 2)) + '%'

        def file_name(path):
            return path.split('/')[-1]

        total = 0
        tested = 0
        for path, stat in self.data['stats'].items():
            assert stat['total']
            table.add_row([file_name(path), stat['total'], stat['tested'],
                                     percent(stat['tested'], stat['total'])])
            total += stat['total']
            tested += stat['tested']

        assert total
        table.add_row(['Total', total, tested, percent(tested, total)])
        print(table)


global_context = GlobalContext()

# Behave function overloading
# ===========================


def before_scenario(context, scenario):
    global_context.reset_promql_test()


def after_scenario(context, scenario):
    assert global_context.promql_expr_test.run_promtool()


def after_all(context):
    global_context.print_query_stats()


@given("the following series")
def step_impl(context):
    for row in context.table:
        metric = row['metrics']
        value = row['values']
        global_context.promql_expr_test.add_series(metric, value)


@when('evaluation interval is `{interval}`')
def step_impl(context, interval):
    interval_without_unit, unit = resolve_time_and_unit(interval)
    if interval_without_unit is None:
        raise ValueError(f'Invalid interval time: {interval_without_unit}. ' +
                           'A valid time looks like "1m" where you have a number plus a unit')
    global_context.promql_expr_test.set_evaluation_interval(interval_without_unit, unit)


@when('interval is `{interval}`')
def step_impl(context, interval):
    interval_without_unit, unit = resolve_time_and_unit(interval)
    if interval_without_unit is None:
        raise ValueError(f'Invalid interval time: {interval_without_unit}. ' +
                           'A valid time looks like "1m" where you have a number plus a unit')
    global_context.promql_expr_test.set_interval(interval_without_unit, unit)


@when('evaluation time is `{eval_time}`')
def step_impl(context, eval_time):
    eval_time_without_unit, unit = resolve_time_and_unit(eval_time)
    if eval_time_without_unit is None:
        raise ValueError(f'Invalid evaluation time: {eval_time}. ' +
                           'A valid time looks like "1m" where you have a number plus a unit')
    global_context.promql_expr_test.set_eval_time(eval_time_without_unit, unit)


@when('variable `{variable}` is `{value}`')
def step_impl(context, variable, value):
    global_context.promql_expr_test.set_variable(variable, value)


@then('Grafana panel `{panel_name}` with legend `{legend}` shows')
def step_impl(context, panel_name, legend):
    """
    This step can have an empty legend. As 'behave' doesn't provide a way
    to say it's empty we use EMPTY to mark as empty.
    """
    if legend == "EMPTY":
        legend = ''
    query_id = panel_name + '-' + legend
    if query_id not in global_context.query_map:
        print(f"QueryMap: {global_context.query_map}")
        raise KeyError((f'Query with legend {legend} in panel "{panel_name}"'
                           'couldn\'t be found'))

    expr = global_context.query_map[query_id]['query']
    global_context.promql_expr_test.set_expression(expr)
    for row in context.table:
        metric = row['metrics']
        value = row['values']
        global_context.promql_expr_test.add_exp_samples(metric, float(value))
    path = global_context.query_map[query_id]['path']
    global_context.data['stats'][path]['tested'] += 1


@then('query `{query}` produces')
def step_impl(context, query):
    global_context.promql_expr_test.set_expression(query)
    for row in context.table:
        metric = row['metrics']
        value = row['values']
        global_context.promql_expr_test.add_exp_samples(metric, float(value))
