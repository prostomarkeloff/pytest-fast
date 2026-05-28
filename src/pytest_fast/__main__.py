"""`python -m pytest_fast` entry — proxies into `pytest_fast.main(argv)`.

Used by `_spawn_daemon`/`_spawn_watcher` for self-exec: the subprocess gets
`[sys.executable, "-m", "pytest_fast", "--serve", ...]`, Python executes THIS
file with `__name__ == "__main__"`. The package's `__init__.py` is imported
BEFORE this file (standard `python -m pkg` behavior), where the preload guard
`__name__ == "pytest_fast"` + env `_PYTEST_FAST_COLLECT` fires.
"""

from __future__ import annotations

import sys

from pytest_fast import main

raise SystemExit(main(sys.argv[1:]))
