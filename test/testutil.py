
import subprocess as sp

def run_m3(*args):
    cl = ['m3'] + list(args)
    P = sp.Popen(cl, stdout=sp.PIPE, stderr=sp.PIPE)
    out, err = P.communicate()
    assert P.returncode == 0
    return out.decode(), err.decode()
