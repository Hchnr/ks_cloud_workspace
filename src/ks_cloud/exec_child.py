"""Linux parent-death guard that also survives exec into ks3util."""
import ctypes
import os
import signal
import sys


if __name__ == '__main__':
    expected_parent = int(sys.argv[1])
    if ctypes.CDLL(None, use_errno=True).prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), 'PR_SET_PDEATHSIG failed')
    if os.getppid() != expected_parent:
        raise SystemExit('parent exited before guard initialization')
    os.execv(sys.argv[2], sys.argv[2:])
