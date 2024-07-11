import re
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from typing import Any, List

import yaml

from .util import replace_grafana_expr_variables


@dataclass
class InputSeries:
    series: str = ''
    values: str = ''

@dataclass
class ExprSample:
    labels: str = ''
    value: float = -1

@dataclass
class PromqlExprTest:
    expr: str = ''
    eval_time: str = '1m'
    exp_samples: List[ExprSample] = field(default_factory=list)

@dataclass
class Test:
    interval: str = '1m'
    input_series: List[InputSeries] = field(default_factory=list)
    promql_expr_test: List[PromqlExprTest] = field(default_factory=list)


@dataclass
class TestFile:
    evaluation_interval: str = '1m'
    tests: List[Test] = field(default_factory=list)


class PromqlTest:
    """
    Base class to provide prometheus query test capabilities. After setting up
    the query test with its input and expected output it's expected to run promtool.

    https://prometheus.io/docs/prometheus/latest/configuration/unit_testing_rules/#test-yml

    The workflow of testing would be something like:

        # add prometheus query to test
        self.set_expression('node_bonding_slaves > 0')

        # add some prometheus input series
        self.add_series('node_bonding_slaves{master="bond0"}', '2')
        self.add_series('node_bonding_slaves{master="bond1"}', '3')
        self.add_series('node_network_receive_bytes{instance="127.0.0.1",
            device="eth1"}', "10 100 230 22")

        # expected output of the query
        self.add_exp_samples('node_bonding_slaves{master="bond0"}', 2)
        self.add_exp_samples('node_bonding_slaves{master="bond1"}', 3)

        # at last, always call promtool with:
        self.assertTrue(self.run_promtool())
        # assertTrue means it expect promtool to succeed
    """

    def __init__(self):
        self.test_output_file = tempfile.NamedTemporaryFile('w+')

        self.test_file = TestFile()
        self.test = Test()
        self.promql_expr_test = PromqlExprTest()
        self.test.promql_expr_test.append(self.promql_expr_test)
        self.test_file.tests.append(self.test)

        self.variables = {}

    def __del__(self):
        self.test_output_file.close()


    def set_evaluation_interval(self, interval: int, unit: str = 'm') -> None:
        """
        Set the evaluation interval of the time series

        Args:
            interval (int): number of units.
            unit (str): unit type: 'ms', 's', 'm', etc...
        """
        self.test_file.evaluation_interval = f'{interval}{unit}'

    def set_interval(self, interval: int, unit: str = 'm') -> None:
        """
        Set the duration of the time series

        Args:
            interval (int): number of units.
            unit (str): unit type: 'ms', 's', 'm', etc...
        """
        self.test.interval = f'{interval}{unit}'

    def set_expression(self, expr: str) -> None:
        """
        Set the prometheus expression/query used to filter data.

        Args:
             expr(str): expression/query.
        """
        self.promql_expr_test.expr = expr

    def add_series(self, series: str, values: str) -> None:
        """
        Add a series to the input.

        Args:
             series(str): Prometheus series.
             Notation: '<metric name>{<label name>=<label value>, ...}'
             values(str): Value of the series.
        """
        input_series = InputSeries(series=series, values=values)
        self.test.input_series.append(input_series)

    def set_eval_time(self, eval_time: int, unit: str = 'm') -> None:
        """
        Set the time when the expression will be evaluated

        Args:
             interval (int): number of units.
             unit (str): unit type: 'ms', 's', 'm', etc...
        """
        self.promql_expr_test.eval_time = f'{eval_time}{unit}'

    def add_exp_samples(self, sample: str, values: Any) -> None:
        """
        Add an expected sample/output of the query given the series/input

        Args:
             sample(str): Expected sample.
             Notation: '<metric name>{<label name>=<label value>, ...}'
             values(Any): Value of the sample.
        """
        expr_sample = ExprSample(labels=sample, value=values)
        self.promql_expr_test.exp_samples.append(expr_sample)

    def set_variable(self, variable: str, value: str):
        """
        If a query makes use of grafonnet variables, for example
        '$osd_hosts', you should change this to a real value. Example:


        > self.set_expression('node_bonding_slaves{master="$osd_hosts"} > 0')
        > self.set_variable('osd_hosts', '127.0.0.1')
        > print(self.query)
        > node_bonding_slaves{master="127.0.0.1"} > 0

        Args:
             variable(str): Variable name
             value(str): Value to replace variable with

        """
        self.variables[variable] = value

    def run_promtool(self):
        """
        Run promtool to test the query after setting up the input, output
        and extra parameters.

        Returns:
             bool: True if successful, False otherwise.
        """

        for variable, value in self.variables.items():
            expr = self.promql_expr_test.expr
            new_expr = replace_grafana_expr_variables(expr, variable, value)
            self.set_expression(new_expr)

        test_as_dict = asdict(self.test_file)
        yaml.dump(test_as_dict, self.test_output_file)

        args = f'promtool test rules {self.test_output_file.name}'.split()
        try:
            subprocess.run(args, check=True)
            return True
        except subprocess.CalledProcessError as process_error:
            print(yaml.dump(test_as_dict))
            print(process_error.stderr)
            return False
