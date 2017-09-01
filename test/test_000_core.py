
from testutil import run_m3

def test_dummy():
    assert 1 == 1

def test_m3_run():
    out, err = run_m3()
    assert 'usage: m3' in out
