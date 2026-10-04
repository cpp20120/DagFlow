#!/usr/bin/env python3

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate project documentation using Doxygen."
    )

    parser.add_argument(
        "--project-name",
        required=True,
    )

    parser.add_argument(
        "--project-root",
        type=Path,
        required=True,
    )

    parser.add_argument(
        "--doxyfile-in",
        type=Path,
        required=True,
    )

    parser.add_argument(
        "--output",
        type=Path,
        required=True,
    )

    parser.add_argument(
        "--sources-file",
        type=Path,
        required=True,
    )

    parser.add_argument(
        "--doxygen",
        type=Path,
        required=True,
    )

    parser.add_argument(
        "--dot",
        type=Path,
        required=True,
    )

    return parser.parse_args()


def read_sources(path: Path) -> list[Path]:
    sources = []

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()

        if not line:
            continue

        sources.append(Path(line).resolve())

    return sources


def doxy_quote(value: str | Path) -> str:
    value = Path(value).as_posix() if isinstance(value, Path) else value

    value = value.replace("\\", "\\\\")
    value = value.replace('"', '\\"')

    return f'"{value}"'


def replace_doxygen_tags(
        template: str,
        values: dict[str, str],
) -> str:
    """
    Replace Doxygen assignments while correctly removing old multiline
    assignments such as:

        FILE_PATTERNS = *.h \
                        *.hpp \
                        *.cpp

    Missing tags are appended to the generated configuration.
    """

    lines = template.splitlines()
    output: list[str] = []
    replaced: set[str] = set()

    index = 0

    while index < len(lines):
        line = lines[index]

        matched_key = None

        for key in values:
            if re.match(
                    rf"^\s*{re.escape(key)}\s*=",
                    line,
                    flags=re.IGNORECASE,
            ):
                matched_key = key
                break

        if matched_key is None:
            output.append(line)
            index += 1
            continue

        output.append(f"{matched_key} = {values[matched_key]}")
        replaced.add(matched_key)

        # Remove continuation lines belonging to the old assignment.
        continued = line.rstrip().endswith("\\")
        index += 1

        while continued and index < len(lines):
            continued = lines[index].rstrip().endswith("\\")
            index += 1

    missing = values.keys() - replaced

    if missing:
        output.append("")
        output.append("# Values injected by generate_docs.py")

        for key in values:
            if key in missing:
                output.append(f"{key} = {values[key]}")

    return "\n".join(output) + "\n"


def run_checked(
        command: list[str | Path],
        *,
        cwd: Path | None = None,
) -> None:
    printable = " ".join(str(arg) for arg in command)
    print(f"+ {printable}")

    subprocess.run(
        [str(arg) for arg in command],
        cwd=cwd,
        check=True,
    )


def generate_docs(args: argparse.Namespace) -> Path:
    project_root = args.project_root.resolve()
    doxyfile_in = args.doxyfile_in.resolve()
    output_dir = args.output.resolve()
    doxygen = args.doxygen.resolve()
    dot = args.dot.resolve()

    sources = read_sources(args.sources_file)

    if not sources:
        raise RuntimeError("No source files were supplied by CMake")

    if not doxyfile_in.is_file():
        raise RuntimeError(
            f"Doxyfile template does not exist: {doxyfile_in}"
        )

    if not doxygen.is_file():
        raise RuntimeError(
            f"Doxygen executable does not exist: {doxygen}"
        )

    if not dot.is_file():
        raise RuntimeError(
            f"Graphviz dot executable does not exist: {dot}"
        )

    print(f"Project: {args.project_name}")
    print(f"Project root: {project_root}")
    print(f"Doxygen: {doxygen}")
    print(f"Graphviz dot: {dot}")
    print(f"Documentation inputs: {len(sources)}")

    # The output belongs to the build tree, so recreating it is fine.
    if output_dir.exists():
        shutil.rmtree(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    input_value = " ".join(doxy_quote(source) for source in sources)

    config = {
        "PROJECT_NAME": doxy_quote(args.project_name),
        "PROJECT_BRIEF": doxy_quote(
            f"{args.project_name} API Documentation"
        ),

        "INPUT": input_value,
        "OUTPUT_DIRECTORY": doxy_quote(output_dir),

        "RECURSIVE": "NO",

        "FILE_PATTERNS": (
            "*.c *.cc *.cxx *.cpp *.c++ "
            "*.h *.hh *.hxx *.hpp *.h++ "
            "*.py"
        ),

        "EXTRACT_ALL": "YES",
        "EXTRACT_PRIVATE": "YES",
        "EXTRACT_STATIC": "YES",
        "EXTRACT_PACKAGE": "YES",

        "HIDE_UNDOC_MEMBERS": "NO",
        "HIDE_UNDOC_CLASSES": "NO",

        "SOURCE_BROWSER": "YES",

        "REFERENCED_BY_RELATION": "YES",
        "REFERENCES_RELATION": "YES",

        "GENERATE_TREEVIEW": "YES",

        "ENABLE_PREPROCESSING": "YES",
        "MACRO_EXPANSION": "YES",
        "EXPAND_ONLY_PREDEF": "YES",
        "PREDEFINED": "DOXYGEN_SHOULD_SKIP_THIS",

        "HAVE_DOT": "YES",
        "DOT_PATH": doxy_quote(dot.parent),

        "DOT_IMAGE_FORMAT": "svg",
        "INTERACTIVE_SVG": "YES",
        "DOT_TRANSPARENT": "YES",

        "CLASS_GRAPH": "YES",
        "COLLABORATION_GRAPH": "YES",
        "GROUP_GRAPHS": "YES",
        "UML_LOOK": "YES",

        "CALL_GRAPH": "YES",
        "CALLER_GRAPH": "YES",

        "GRAPHICAL_HIERARCHY": "YES",
        "DIRECTORY_GRAPH": "YES",

        "DOT_GRAPH_MAX_NODES": "100",
        "MAX_DOT_GRAPH_DEPTH": "3",
        "DOT_MULTI_TARGETS": "YES",
        "GENERATE_LEGEND": "YES",
        "DOT_CLEANUP": "YES",
    }

    template = doxyfile_in.read_text(encoding="utf-8")

    generated = replace_doxygen_tags(
        template,
        config,
    )

    doxyfile = output_dir / "Doxyfile"
    doxyfile.write_text(
        generated,
        encoding="utf-8",
    )

    version = subprocess.run(
        [str(doxygen), "--version"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    print(f"Using Doxygen {version}")

    run_checked(
        [doxygen, doxyfile],
        cwd=project_root,
    )

    index = output_dir / "html" / "index.html"

    if not index.is_file():
        raise RuntimeError(
            f"Doxygen completed but did not produce {index}"
        )

    return index


def main() -> int:
    args = parse_args()

    try:
        index = generate_docs(args)
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(
            f"Documentation generation failed: {exc}",
            file=sys.stderr,
        )
        return 1

    print(f"Documentation generated successfully: {index}")
    return 0


if __name__ == "__main__":
    sys.exit(main())