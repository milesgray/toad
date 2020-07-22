import pathlib
import importlib

def get_files(path):
    path = pathlib.Path(path) if isinstance(path, str) else path
    files = {}    
    for p in path.glob("*.py"):
        if "checkpoint" not in p.stem and not p.stem.startswith("_") and p.stem != "manager":
            files[p.stem] = str(p)

    return files

def get_child_folders(location):
    folders = {}
    path = pathlib.Path(location)
    folder_paths = [p for p in path.glob("*/**") if 
                        not p.stem.startswith("_") and 
                        not p.stem.startswith(".") and 
                        '-' not in str(p) and
                        p.parent == path]
    for fp in folder_paths:
        folders[fp.stem] = get_files(fp)
    
    return folders

def get_spec(file, name):
    return importlib.util.spec_from_file_location(name, file)
