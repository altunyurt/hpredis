"""Run a redis-py benchmark with hpredis standing in for hiredis.

From a redis-py checkout:

    PYTHONPATH=/path/to/hpredis/src python src/benchmark/with_hpredis.py \
        -m benchmarks.basic_operations -n 100000 -P 1 -s 2

or a script (its directory goes on sys.path, like `python script.py`):

    PYTHONPATH=/path/to/hpredis/src python src/benchmark/with_hpredis.py \
        benchmarks/connection_handshake.py

redis-py imports `hiredis` once, then builds `hiredis.Reader` per connection
and calls `hiredis.pack_command` per command, so replacing the module
attributes before the benchmark runs is the whole slide-in.  Set HP_PACK=0
to keep hiredis's pack_command (parser-only comparison).
"""

import os
import runpy
import sys

import hpredis

try:
    import hiredis
except ImportError:
    # no hiredis installed: let redis-py's `import hiredis` find hpredis
    hiredis = None

if hiredis is None:
    sys.modules["hiredis"] = hpredis
    print("hpredis slide-in: sys.modules['hiredis']", file=sys.stderr)
else:
    hiredis.Reader = hpredis.Reader
    hiredis.PushNotification = hpredis.PushNotification
    pack = os.environ.get("HP_PACK", "1") != "0"
    if pack:
        hiredis.pack_command = hpredis.pack_command
    print("hpredis slide-in: Reader, PushNotification"
          + (", pack_command" if pack else ""), file=sys.stderr)

args = sys.argv[1:]
if args and args[0] == "-m":
    module, *rest = args[1:]
    sys.path.insert(0, os.getcwd())  # the checkout, not this script's dir
    sys.argv = [module] + rest
    runpy.run_module(module, run_name="__main__", alter_sys=True)
else:
    script, *rest = args
    sys.path.insert(0, os.path.dirname(os.path.abspath(script)))
    sys.argv = [script] + rest
    runpy.run_path(script, run_name="__main__")
