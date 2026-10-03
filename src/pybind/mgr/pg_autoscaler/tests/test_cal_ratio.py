from pg_autoscaler import effective_target_ratio
from pytest import approx


def check_simple_ratio(target_ratio, tot_ratio):
    etr = effective_target_ratio(target_ratio, tot_ratio, 0, 0)
    if tot_ratio > 1.0:
        assert (target_ratio / tot_ratio) == approx(etr)
    else:
        assert target_ratio == approx(etr)
    return etr


def test_simple():
    # Once the ratios add up to more than 1.0 they are relative weights, so
    # scaling every ratio by the same factor leaves the result alone.
    etr1 = check_simple_ratio(2, 9)
    etr2 = check_simple_ratio(20, 90)
    assert etr1 == approx(etr2)

    etr1 = check_simple_ratio(1, 2)
    etr2 = check_simple_ratio(0.5, 1.0)
    assert etr1 == approx(etr2)


def test_ratios_below_one_are_taken_as_given():
    # A lone pool asking for a tenth of the cluster gets a tenth of it. The
    # ratios only become relative weights once they add up to more than one.
    assert effective_target_ratio(0.1, 0.1, 0, 0) == approx(0.1)
    assert effective_target_ratio(0.5, 0.8, 0, 0) == approx(0.5)
    assert effective_target_ratio(0.3, 0.8, 0, 0) == approx(0.3)
    assert effective_target_ratio(0.9, 0.9, 0, 0) == approx(0.9)


def test_ratios_above_one_are_weights():
    assert effective_target_ratio(2, 9, 0, 0) == approx(2 / 9)
    assert effective_target_ratio(20, 90, 0, 0) == approx(2 / 9)
    assert effective_target_ratio(1, 2, 0, 0) == approx(0.5)
    # Exactly one is the boundary: nothing to scale down.
    assert effective_target_ratio(0.5, 1.0, 0, 0) == approx(0.5)


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
