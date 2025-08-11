#!/usr/bin/env python3
import os
import shutil
import subprocess
import sys
import re
from pathlib import Path

def check_graphviz_installed():
    try:
        result = subprocess.run(['dot', '-V'],
                                capture_output=True,
                                text=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False

def check_doxygen_installed():
    try:
        result = subprocess.run(['doxygen', '--version'],
                                capture_output=True,
                                text=True)
        return result.returncode == 0
    except FileNotFoundError:
        return False

def find_source_files(project_root):
    input_dirs = [
        project_root / "include",
        project_root / "src",
        *list((project_root / "lib").glob("*/include")),
        *list((project_root / "lib").glob("*/src"))
    ]

    extensions = ['*.h', '*.hpp', '*.c', '*.cpp', '*.cc', '*.cxx', '*.py', '*.java']
    source_files = []

    for directory in input_dirs:
        if directory.exists():
            for ext in extensions:
                source_files.extend(directory.rglob(ext))

    return sorted(source_files)

def get_project_name_from_cmake(project_root):
    cmake_file = project_root / "CMakeLists.txt"
    if not cmake_file.exists():
        return "API Documentation"

    with open(cmake_file, 'r', encoding='utf-8') as f:
        content = f.read()

    match = re.search(r'project\s*\(\s*([^\s\)]+)', content, re.IGNORECASE)
    if match:
        return match.group(1)

    return "API Documentation"

def generate_doxygen():
    project_root = Path(__file__).parent.parent.resolve()
    docs_dir = project_root / "docs"
    doxyfile_in = docs_dir / "Doxyfile.in"
    doxygen_output_dir = docs_dir / "doxygen_output" 

    if doxygen_output_dir.exists():
        shutil.rmtree(doxygen_output_dir)
    doxygen_output_dir.mkdir(parents=True)

    project_name = get_project_name_from_cmake(project_root)
    print(f"Detected project name: {project_name}")

    has_graphviz = check_graphviz_installed()
    print(f"Graphviz available: {'YES' if has_graphviz else 'NO'}")

    source_files = find_source_files(project_root)
    if not source_files:
        print("Error: No source files found for documentation")
        return 1

    print(f"Found {len(source_files)} source files for documentation")

    with open(doxyfile_in, 'r', encoding='utf-8') as f:
        doxyfile_content = f.read()

    config = {
        'PROJECT_NAME': project_name,
        'PROJECT_BRIEF': f"{project_name} API Documentation",
        'INPUT': ' '.join(f'"{f}"' for f in source_files),
        'OUTPUT_DIRECTORY': str(doxygen_output_dir), 
        'RECURSIVE': 'YES',
        'EXTRACT_ALL': 'YES',
        'EXTRACT_PRIVATE': 'YES',
        'EXTRACT_STATIC': 'YES',
        'SOURCE_BROWSER': 'YES',
        'REFERENCED_BY_RELATION': 'YES',
        'REFERENCES_RELATION': 'YES',
        'GENERATE_TREEVIEW': 'YES',
        'FILE_PATTERNS': '*.c *.cc *.cxx *.cpp *.c++ *.h *.hh *.hxx *.hpp *.h++ *.py',
        'ENABLE_PREPROCESSING': 'YES',
        'MACRO_EXPANSION': 'YES',
        'EXPAND_ONLY_PREDEF': 'YES',
        'PREDEFINED': 'DOXYGEN_SHOULD_SKIP_THIS',
        'EXTRACT_PACKAGE': 'YES',
        'HIDE_UNDOC_MEMBERS': 'NO',
        'HIDE_UNDOC_CLASSES': 'NO',

        'HAVE_DOT': 'YES' if has_graphviz else 'NO',
        'DOT_IMAGE_FORMAT': 'svg',
        'INTERACTIVE_SVG': 'YES',
        'DOT_TRANSPARENT': 'YES',
        'CLASS_GRAPH': 'YES',
        'COLLABORATION_GRAPH': 'YES',
        'GROUP_GRAPHS': 'YES',
        'UML_LOOK': 'YES',
        'CALL_GRAPH': 'YES',
        'CALLER_GRAPH': 'YES',
        'GRAPHICAL_HIERARCHY': 'YES',
        'DIRECTORY_GRAPH': 'YES',
        'DOT_GRAPH_MAX_NODES': '100',
        'MAX_DOT_GRAPH_DEPTH': '3',
        'DOT_MULTI_TARGETS': 'YES',
        'GENERATE_LEGEND': 'YES',
        'DOT_CLEANUP': 'YES'
    }

    for key, value in config.items():
        pattern = f"{key}\\s*=.*"
        replacement = f"{key} = {value}"
        lines = []
        for line in doxyfile_content.split('\n'):
            if re.match(pattern, line):
                lines.append(replacement)
            else:
                lines.append(line)
        doxyfile_content = '\n'.join(lines)

    doxyfile = doxygen_output_dir / "Doxyfile"
    with open(doxyfile, 'w', encoding='utf-8') as f:
        f.write(doxyfile_content)

    try:
        doxygen_exec = 'doxygen.exe' if os.name == 'nt' else 'doxygen'
        result = subprocess.run([doxygen_exec, '--version'],
                                check=True,
                                capture_output=True,
                                text=True)
        print(f"Using Doxygen version: {result.stdout.strip()}")
    except Exception as e:
        print(f"Error: {e}")
        return 1

    print("\nGenerating documentation...")
    try:
        subprocess.run([doxygen_exec, str(doxyfile)],
                       cwd=str(project_root),
                       check=True)
    except subprocess.CalledProcessError as e:
        print(f"Generation failed: {e}")
        return 1

    index_html = doxygen_output_dir / "html" / "index.html"
    if not index_html.exists():
        print("Error: Documentation was not generated")
        return 1

    print(f"\nDocumentation successfully generated at:\n{index_html}")

    if has_graphviz:
        print("\nClass diagrams and other graphs were generated")
    else:
        print("\nNote: Install Graphviz (dot) for class diagrams generation")

    try:
        if sys.platform == "win32":
            os.startfile(index_html)
        elif sys.platform == "darwin":
            subprocess.run(["open", str(index_html)], check=True)
        else:
            subprocess.run(["xdg-open", str(index_html)], check=True)
    except Exception as e:
        print(f"Note: Could not open browser: {e}")

if __name__ == "__main__":
    sys.exit(generate_doxygen())