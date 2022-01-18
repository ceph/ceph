import unittest

from ..module import CLI, BenchmarkException, HandleCommandResult


class BenchmarkRunnerTest(unittest.TestCase):
    def setUp(self):
        self.cli = CLI('CLI', 0, 0)

    def test_number_of_calls_on_start_fails(self):
        with self.assertRaises(BenchmarkException) as ctx:
            self.cli.benchmark(0, 10, 'list_servers', [])
        self.assertEqual(str(ctx.exception),
                         "Number of calls and number "
                         "of parallel calls must be greater than 0")

    def test_number_of_parallel_calls_on_start_fails(self):
        with self.assertRaises(BenchmarkException) as ctx:
            self.cli.benchmark(100, 0, 'list_servers', [])
        self.assertEqual(str(ctx.exception),
                         "Number of calls and number "
                         "of parallel calls must be greater than 0")

    def test_number_of_parallel_calls_on_start_works(self):
        CLI.benchmark(10, 10, "get", "osd_map")

    def test_function_name_fails(self):
        for iterations in [0, 1]:
            threads = 0 if iterations else 1
            with self.assertRaises(BenchmarkException) as ctx:
                self.cli.benchmark(iterations, threads, 'fake_method', [])
            self.assertEqual(str(ctx.exception),
                             "Number of calls and number "
                             "of parallel calls must be greater than 0")
        result: HandleCommandResult = self.cli.benchmark(1, 1, 'fake_method', [])
        self.assertEqual(result.stderr, "Could not find the public "
                         "function you are requesting")

    def test_function_name_works(self):
        CLI.benchmark(10, 10, "get", "osd_map")
