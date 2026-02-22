"""
zUtility/generate_clean_requirements.py
------------------------------------------------
Generate a minimal requirements.txt by scanning imports
in project scripts and checking installed package versions.
Ignores standard library modules and project folders.
"""

import os
import ast
import sys
from pathlib import Path

# ------------------------
# Folders to scan
# ------------------------
SCAN_FOLDERS = ["api", "contracts", "jobs", "db"]
EXCLUDE_FOLDERS = {".venv", "__pycache__", ".git", ".idea", "zUtility"}

# ------------------------
# Standard library modules (Python 3.12)
# ------------------------
STD_LIBS = set(sys.builtin_module_names) | {
    "os", "sys", "json", "ast", "pathlib", "datetime", "typing", "re", "csv",
    "dataclasses", "uuid", "types", "logging", "functools", "shutil", "subprocess"
}

# ------------------------
# Project modules to ignore
# ------------------------
PROJECT_MODULES = {"api", "contracts", "db", "jobs", "logs", "zUtility"}

# ------------------------
# Helper functions
# ------------------------
try:
    from importlib.metadata import version, PackageNotFoundError
except ImportError:
    from importlib_metadata import version, PackageNotFoundError

def find_imports_in_file(filepath):
    """Return set of imported module names in a Python file"""
    with open(filepath, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=filepath)
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                imports.add(n.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.add(node.module.split(".")[0])
    return imports

def scan_project_for_imports(root_path):
    """Scan all python files in SCAN_FOLDERS"""
    imported_modules = set()
    for folder in SCAN_FOLDERS:
        folder_path = Path(root_path) / folder
        if not folder_path.exists():
            continue
        for dirpath, dirnames, filenames in os.walk(folder_path):
            dirnames[:] = [d for d in dirnames if d not in EXCLUDE_FOLDERS]
            for file in filenames:
                if file.endswith(".py"):
                    file_path = Path(dirpath) / file
                    imported_modules.update(find_imports_in_file(file_path))
    # Remove standard library and project modules
    used_packages = {m for m in imported_modules if m not in STD_LIBS and m not in PROJECT_MODULES}
    return used_packages

def generate_requirements(root_path, output_file="requirements.txt"):
    modules = scan_project_for_imports(root_path)
    lines = []
    for m in sorted(modules):
        try:
            v = version(m)
            lines.append(f"{m}=={v}")
        except PackageNotFoundError:
            lines.append(f"{m}  # package not installed")
    # Add Python version at the top
    lines.insert(0, f"# Python {sys.version_info.major}.{sys.version_info.minor}")
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"[INFO] {output_file} generated with {len(modules)} packages.")

# ------------------------
# Main execution
# ------------------------
if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).parent.parent
    generate_requirements(PROJECT_ROOT)