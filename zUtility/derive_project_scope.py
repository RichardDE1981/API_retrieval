import os
import ast
import sys

# ------------------------
# Determine top-level project root
# ------------------------
current_path = os.path.abspath(os.path.dirname(__file__))

while True:
    if os.path.exists(os.path.join(current_path, "contracts")):
        PROJECT_ROOT = current_path
        break
    parent = os.path.dirname(current_path)
    if parent == current_path:
        PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
        break
    current_path = parent

print(f"[INFO] Scanning project root: {PROJECT_ROOT}")

# ------------------------
# Exclusions
# ------------------------
EXCLUDE_FOLDERS = {".venv", "__pycache__", ".git", ".idea", "zUtility"}
EXCLUDE_FILES = {".DS_Store"}
PYTHON_EXTENSIONS = {".py"}
OTHER_EXTENSIONS = {".sql", ".md", ".yaml", ".yml", ".json"}

# ------------------------
# Helper functions
# ------------------------
def scan_python_file(filepath):
    """Return sections of a Python file: docstring, classes, functions, top-level variables"""
    sections = []

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            source = f.read()
        tree = ast.parse(source)
    except Exception as e:
        sections.append(f"[ERROR PARSING FILE]: {e}")
        return sections

    # Module docstring
    mod_doc = ast.get_docstring(tree)
    if mod_doc:
        sections.append(f"Module docstring: {mod_doc.splitlines()[0]}{'...' if len(mod_doc.splitlines())>1 else ''}")

    # Top-level nodes
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            doc = ast.get_docstring(node)
            sections.append(f"Class {node.name}: {doc.splitlines()[0] if doc else '<no docstring>'}")
        elif isinstance(node, ast.FunctionDef):
            doc = ast.get_docstring(node)
            sections.append(f"Function {node.name}: {doc.splitlines()[0] if doc else '<no docstring>'}")
        elif isinstance(node, ast.Assign):
            targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
            for t in targets:
                val_preview = ""
                if isinstance(node.value, ast.Constant):
                    val_preview = str(node.value.value)
                    if len(val_preview) > 50:
                        val_preview = val_preview[:50] + "..."
                sections.append(f"Variable {t}: {val_preview}")

    return sections

def scan_project(root):
    """Scan folders and files"""
    scope = {}

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_FOLDERS]
        rel_path = os.path.relpath(dirpath, root)
        if rel_path == ".":
            rel_path = ""
        files = [f for f in filenames if f not in EXCLUDE_FILES]
        if files:
            scope[rel_path] = files

    return scope

def format_scope(scope):
    """Return project scope as a string instead of printing"""
    lines = []
    lines.append("project_root/\n")
    for folder, files in sorted(scope.items()):
        prefix = f"├─ {folder}/" if folder else "├─ "
        lines.append(prefix)
        for f in sorted(files):
            file_path = os.path.join(PROJECT_ROOT, folder, f)
            ext = os.path.splitext(f)[1].lower()
            if ext in PYTHON_EXTENSIONS:
                sections = scan_python_file(file_path)
                lines.append(f"│   ├─ {f}")
                for s in sections:
                    lines.append(f"│   │   ├─ {s}")
            else:
                lines.append(f"│   ├─ {f}")
    return "\n".join(lines)

# ------------------------
# Main execution
# ------------------------
if __name__ == "__main__":
    derived_scope = scan_project(PROJECT_ROOT)
    scope_text = format_scope(derived_scope)

    # Print to console
    print(scope_text)

    # Write to SCOPE.txt in project root
    scope_file_path = os.path.join(PROJECT_ROOT, "SCOPE.txt")
    with open(scope_file_path, "w", encoding="utf-8") as f:
        f.write(scope_text)

    print(f"\n[INFO] Scope written to {scope_file_path}")