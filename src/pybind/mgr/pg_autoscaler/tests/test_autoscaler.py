from pg_autoscaler import effective_target_ratio, PgAutoscaler, nearest_power_of_two
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

def test_pg_move():
    pool_stats = {'rd': 30, 'wr': 50}
    pool_id = 5
    final_pg_target = 50
    old_values = {1: {'total_requests': 0, 'potential_moved': 0, 'current_moved': 0, 'potential_add': 0,
                      'current_add': 0}}
    old_values, final_pg_target = PgAutoscaler.check_pool_activity(1, pool_stats, pool_id, final_pg_target, old_values)
    assert old_values[pool_id]['total_requests'] == pool_stats['rd'] + pool_stats['wr']
    PgAutoscaler.check_pool_activity(1, pool_stats, pool_id, final_pg_target, old_values)
    assert old_values[pool_id]['potential_moved'] == 1
    assert old_values[1]['potential_add'] == 1
    old_values = {1: {'total_requests': 0, 'potential_moved': 0, 'current_moved': 0, 'potential_add': 64,
                      'current_add': 0}, 5: {'total_requests': 0, 'potential_moved': 32, 'current_moved': 0,
                      'potential_add': 0, 'current_add': 0}, 6: {'total_requests': 0, 'potential_moved': 32,
                      'current_moved': 0, 'potential_add': 0, 'current_add': 0}}
    final_pg_target = 64
    test_target = final_pg_target
    old_values, final_pg_target = PgAutoscaler.check_pool_activity(0, pool_stats, 1, test_target, old_values)
    assert test_target + 64 == final_pg_target
    final_pg_target = 64
    old_values, final_pg_target1 = PgAutoscaler.check_pool_activity(0, pool_stats, pool_id, final_pg_target, old_values)
    assert test_target - 32 == final_pg_target1

