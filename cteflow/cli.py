"""
cli.py — CLI entry point that ties parser + renderer together.

Usage:
    cteflow <input.sql> [dialect]
    python -m cteflow <input.sql> [dialect]

Output HTML goes to the current working directory.
"""

import sys
import webbrowser
from pathlib import Path

from cteflow.parser import parse_sql
from cteflow.renderer import generate_html

OUTPUT_DIR = Path.cwd() / "output"


def main():
    if len(sys.argv) < 2:
        print("Usage: cteflow <input.sql> [dialect]")
        print("Dialects: snowflake (default), postgres, bigquery, mysql, spark, tsql")
        sys.exit(1)

    sql_file = Path(sys.argv[1])
    if not sql_file.exists():
        print(f"Error: file not found: {sql_file}")
        sys.exit(1)

    dialect = sys.argv[2] if len(sys.argv) > 2 else "snowflake"

    print(f"Parsing {sql_file} (dialect={dialect})...")
    sql = sql_file.read_text()
    graph = parse_sql(sql, dialect)

    cte_names = [n["label"] for n in graph["nodes"] if n["type"] == "cte"]
    sources = [n["label"] for n in graph["nodes"] if n["type"] == "source"]

    print(f"  {len(graph['nodes'])} nodes, {len(graph['edges'])} edges")
    print(f"  CTEs: {', '.join(cte_names)}")
    print(f"  Sources: {', '.join(sources)}")

    output_html = generate_html(graph, sql_file.name, raw_sql=sql)

    OUTPUT_DIR.mkdir(exist_ok=True)
    output_file = OUTPUT_DIR / (sql_file.stem + "_flow.html")
    output_file.write_text(output_html)

    abs_path = output_file.absolute()
    print(f"Graph saved to {output_file}")
    webbrowser.open(f"file://{abs_path}")


if __name__ == "__main__":
    main()
