import os
import sys

sys.path.insert(0, "\\".join(os.path.dirname(__file__).split("\\")[:-1]))
from src.config import BASE_DIR

main_dir = os.path.abspath(os.path.join(BASE_DIR, ".."))
project_dir = BASE_DIR


from src.data.load_raw import load_all
from src.data.compiling_raw_files import compile_all
from src.data.merge_sources import merge_all
from src.data.standard_simple_merged import clean_standard
import warnings

warnings.filterwarnings("ignore")


def run_all():

    print("Loading Data")
    load_all()

    print("Compiling Data")
    compile_all()

    print("Merging Data")
    merge_all()

    print("Standard CSIN Data")
    clean_standard()


if __name__ == "__main__":
    run_all()
