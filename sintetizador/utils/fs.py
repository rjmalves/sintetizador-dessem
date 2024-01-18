from pathlib import Path
import os


class set_directory:
    """
    Directory changing context manager for helping specific cases
    in HPC script executions.
    """

    def __init__(self, path: str):
        self.path = Path(path)
        self.origin = Path().absolute()

    def __enter__(self):
        os.chdir(self.path)

    def __exit__(self, *args, **kwargs):
        os.chdir(self.origin)


def find_file_case_insensitive(path: str, candidate_filename: str) -> str:
    """
    Finds a file in a directory, case insensitive. Returns the full path.
    """
    upper_filename = candidate_filename.upper()
    lower_filename = candidate_filename.lower()
    for try_filename in [candidate_filename, upper_filename, lower_filename]:
        fullpath = Path(path).joinpath(try_filename)
        if fullpath.exists():
            return str(fullpath)
    raise FileNotFoundError(f"File {candidate_filename} not found in {path}")
