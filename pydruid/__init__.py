# Make package global even if used as a sub package to enable short imports.
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
