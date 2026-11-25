"""Pytest bootstrap: make the repository root importable.

The tests import the application as ``app.*``. Running
``python -m pytest`` already puts the root on ``sys.path``; this tiny
bootstrap additionally makes a bare ``pytest`` invocation work regardless
of the caller's working directory.
"""

from __future__ import annotations

import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
