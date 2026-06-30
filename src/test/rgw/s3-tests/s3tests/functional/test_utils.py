from . import utils

def test_generate():
    FIVE_MB = 5 * 1024 * 1024
    assert len(''.join(utils.generate_random(0))) == 0
    assert len(''.join(utils.generate_random(1))) == 1
    assert len(''.join(utils.generate_random(FIVE_MB - 1))) == FIVE_MB - 1
    assert len(''.join(utils.generate_random(FIVE_MB))) == FIVE_MB
    assert len(''.join(utils.generate_random(FIVE_MB + 1))) == FIVE_MB + 1
