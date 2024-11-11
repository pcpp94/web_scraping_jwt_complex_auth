import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)),'..'))
OUTPUTS_DIR = os.path.join(BASE_DIR, 'outputs')
COMPILED_OUTPUTS_DIR = os.path.join(BASE_DIR, 'compiled_outputs')
UTILS_DIR = os.path.join(BASE_DIR, 'utils')
NOTEBOOKS_DIR = os.path.join(BASE_DIR, 'notebooks')