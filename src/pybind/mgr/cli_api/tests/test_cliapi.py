import unittest

from ..module import ThreadedBenchmarkRunner, BenchmarkException


class ThreadedBenchmarkRunnerTest(unittest.TestCase):
    def test_number_of_calls_on_start_fails(self):
        class_threadbenchmarkrunner = ThreadedBenchmarkRunner(0, 10)
        with self.assertRaises(BenchmarkException):
            class_threadbenchmarkrunner.start(None)

    def test_number_of_parallel_calls_on_start_fails(self):
        class_threadbenchmarkrunner = ThreadedBenchmarkRunner(10, 0)
        with self.assertRaises(BenchmarkException):
            class_threadbenchmarkrunner.start(None)

    def test_number_of_parallel_calls_on_start_works(self):
        class_threadbenchmarkrunner = ThreadedBenchmarkRunner(10, 10)

        def dummy_function():
            pass
        class_threadbenchmarkrunner.start(dummy_function)
        assert len(class_threadbenchmarkrunner._self_time) > 0
        assert sum(class_threadbenchmarkrunner._self_time) > 0

    def test_get_stats_works(self):
        class_threadbenchmarkrunner = ThreadedBenchmarkRunner(10, 10)

        def dummy_function():
            for i in range(10):
                pass
        class_threadbenchmarkrunner.start(dummy_function)
        stats = class_threadbenchmarkrunner.get_stats()
        assert stats['avg'] > 0
        assert stats['min'] > 0
        assert stats['max'] > 0
