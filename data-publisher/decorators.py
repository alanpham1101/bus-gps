import os
import shutil
import functools


def clean_folder(folder_path):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            finally:
                if os.path.exists(folder_path):
                    print(f"Cleaning folder: {folder_path}")
                    shutil.rmtree(folder_path, ignore_errors=True)
                    print(f"Folder {folder_path} cleaned successfully.")
                else:
                    print(f"Folder {folder_path} does not exist.")
        return wrapper
    return decorator
