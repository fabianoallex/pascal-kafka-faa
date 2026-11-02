#!/bin/sh
# Fails if any generated file is out of date: the FPCUnit mirrors
# (gen_fpc_mirror.py), the message codecs and the error table
# (gen_messages.py), and the compiled test captures (gen_test_captures.py).
set -e
cd "$(dirname "$0")/.."
python tools/gen_fpc_mirror.py --check
python tools/gen_messages.py --check
python tools/gen_test_captures.py --check
