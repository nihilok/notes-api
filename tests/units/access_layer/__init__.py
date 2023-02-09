import os


def remove_persistence_file(path):
    full_path = str(path) + ".db"
    os.remove(full_path)
