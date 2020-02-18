from pg_autoscaler import effective_target_ratio
from pytest import approx

def check_simple_ratio(target_ratio, tot_ratio):
    etr = effective_target_ratio(target_ratio, tot_ratio, 0, 0)
    assert (target_ratio / tot_ratio) == approx(etr)
    return etr

def test_simple():
    etr1 = check_simple_ratio(0.2, 0.9)
    etr2 = check_simple_ratio(2, 9)
    etr3 = check_simple_ratio(20, 90)
    assert etr1 == approx(etr2)
    assert etr1 == approx(etr3)

    etr = check_simple_ratio(0.9, 0.9)
    assert etr == approx(1.0)
    etr1 = check_simple_ratio(1, 2)
    etr2 = check_simple_ratio(0.5, 1.0)
    assert etr1 == approx(etr2)

def test_total_bytes():
    etr = effective_target_ratio(1, 10, 5, 10)
    assert etr == approx(0.05)
    etr = effective_target_ratio(0.1, 1, 5, 10)
    assert etr == approx(0.05)
    etr = effective_target_ratio(1, 1, 5, 10)
    assert etr == approx(0.5)
    etr = effective_target_ratio(1, 1, 0, 10)
    assert etr == approx(1.0)
    etr = effective_target_ratio(0, 1, 5, 10)
    assert etr == approx(0.0)
    etr = effective_target_ratio(1, 1, 10, 10)
    assert etr == approx(0.0)
