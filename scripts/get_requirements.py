import os
import sys
import pkg_resources

sys.path.insert(0, "\\".join(os.path.dirname(__file__).split("\\")[:-1]))
from src.config import BASE_DIR

main_dir = os.path.abspath(os.path.join(BASE_DIR, ".."))
project_dir = BASE_DIR

initial_modules = set(sys.modules.keys())

from src.client.csin_client import *
from src.data.load_raw import *
from src.data.compiling_raw_files import *
from src.data.merge_sources import *

if __name__ == "__main__":

    # Capture the list of modules after user imports
    final_modules = set(sys.modules.keys())

    # Determine which modules were added
    user_imported_modules = final_modules - initial_modules

    # Filter out standard library modules
    user_imported_modules = {
        name
        for name in user_imported_modules
        if name in pkg_resources.working_set.by_key
    }

    # Get the versions of the imported modules
    versions = {
        pkg.key: pkg.version
        for pkg in pkg_resources.working_set
        if pkg.key in user_imported_modules
    }

    # Write these modules to a requirements.txt file
    with open(os.path.join(BASE_DIR, "requirements.txt"), "w") as f:
        for module in user_imported_modules:
            f.write(f"{module}\n")

    print("User Imported Modules:", user_imported_modules)
