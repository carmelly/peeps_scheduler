"""Test package initialization.

Adds the project root to ``sys.path`` so tests can import modules as a package
regardless of the working directory chosen by pytest.``
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
	sys.path.insert(0, str(ROOT))