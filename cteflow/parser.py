"""
parser.py — Parse SQL and emit a rich JSON graph structure.

Usage (standalone):
    python -m cteflow.parser <input.sql> [dialect] [-o output.json]
"""

import json
import sys
from pathlib import Path

import sqlglot
from sqlglot import exp


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _qualified_name(table: exp.Table) -> str:
    name = table.name
    db = table.args.get("db")
    catalog = table.args.get("catalog")
    if catalog and db:
        return f"{catalog}.{db}.{name}"
    if db:
        return f"{db}.{name}"
    return name


def _extract_dependencies(node: exp.Expression, cte_names: set) -> list:
    seen = {}
    for table in node.find_all(exp.Table):
        short = table.name
        full = _qualified_name(table)
        is_qualified = table.args.get("db") is not None or table.args.get("catalog") is not None
        if is_qualified:
            seen[full] = full
        else:
            seen[short] = full

    result = []
    for key, full in seen.items():
        result.append(key if key in cte_names else full)
    return result


# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------

def _extract_output_columns(node: exp.Expression) -> list:
    """Extract the output column names/aliases from a SELECT."""
    cols = []
    for expr in node.expressions:
        if isinstance(expr, exp.Star):
            # Could be "table.*" or plain "*"
            table = expr.args.get("table")
            cols.append(f"{table.name}.*" if table else "*")
        elif expr.alias:
            cols.append(expr.alias)
        elif isinstance(expr, exp.Column):
            cols.append(expr.name)
        else:
            # Fallback: use the SQL text, truncated
            text = expr.sql()
            cols.append(text if len(text) <= 40 else text[:37] + "...")
    return cols


def _extract_transformation_tags(node: exp.Expression) -> list:
    """Detect what kinds of operations this SELECT performs."""
    tags = []

    # GROUP BY
    if node.args.get("group"):
        tags.append("GROUP BY")

    # DISTINCT
    if node.args.get("distinct"):
        tags.append("DISTINCT")

    # JOINs
    from_clause = node.args.get("from_")
    joins = list(node.find_all(exp.Join))
    if joins:
        join_types = set()
        for j in joins:
            kind = j.args.get("kind") or ""
            side = j.args.get("side") or ""
            label = f"{side} {kind}".strip().upper() or "INNER"
            join_types.add(label)
        for jt in sorted(join_types):
            tags.append(f"{jt} JOIN")

    # WINDOW functions
    if list(node.find_all(exp.Window)):
        tags.append("WINDOW")

    # WHERE
    if node.args.get("where"):
        tags.append("FILTER")

    # HAVING
    if node.args.get("having"):
        tags.append("HAVING")

    # UNION / INTERSECT / EXCEPT
    for union_type in (exp.Union, exp.Intersect, exp.Except):
        if list(node.find_all(union_type)):
            tags.append(union_type.__name__.upper())
            break

    # Subqueries
    subqueries = [s for s in node.find_all(exp.Subquery)
                  if s is not node and not isinstance(s.parent, exp.CTE)]
    if subqueries:
        tags.append("SUBQUERY")

    # ORDER BY
    if node.args.get("order"):
        tags.append("ORDER BY")

    # LIMIT
    if node.args.get("limit"):
        tags.append("LIMIT")

    return tags


def _extract_where_clause(node: exp.Expression, dialect: str) -> str | None:
    where = node.args.get("where")
    if where:
        return where.this.sql(dialect=dialect, pretty=True)
    return None


def _extract_having_clause(node: exp.Expression, dialect: str) -> str | None:
    having = node.args.get("having")
    if having:
        return having.this.sql(dialect=dialect, pretty=True)
    return None


def _extract_joins(node: exp.Expression, dialect: str) -> list:
    """Return list of {type, table, on} for each JOIN."""
    result = []
    for j in node.find_all(exp.Join):
        kind = j.args.get("kind") or ""
        side = j.args.get("side") or ""
        label = f"{side} {kind}".strip().upper() or "INNER"

        table_expr = j.this
        if isinstance(table_expr, exp.Table):
            tname = _qualified_name(table_expr)
            alias = table_expr.alias
        else:
            tname = table_expr.sql(dialect=dialect)
            alias = ""

        on_clause = j.args.get("on")
        on_sql = on_clause.sql(dialect=dialect) if on_clause else ""

        result.append({
            "type": label,
            "table": tname,
            "alias": alias or "",
            "on": on_sql,
        })
    return result


def _extract_group_by_keys(node: exp.Expression, dialect: str) -> list:
    group = node.args.get("group")
    if not group:
        return []
    keys = []
    for expr in group.expressions:
        if isinstance(expr, exp.Column):
            keys.append(expr.name)
        else:
            text = expr.sql(dialect=dialect)
            keys.append(text if len(text) <= 50 else text[:47] + "...")
    return keys


def _extract_window_functions(node: exp.Expression, dialect: str) -> list:
    """Return list of {function, partition_by, order_by} for window fns."""
    result = []
    seen = set()
    for win in node.find_all(exp.Window):
        func = win.this
        func_name = func.sql(dialect=dialect) if func else "?"

        partition = win.args.get("partition_by")
        part_cols = []
        if partition:
            for p in partition:
                part_cols.append(p.name if isinstance(p, exp.Column) else p.sql(dialect=dialect))

        order = win.args.get("order")
        order_cols = []
        if order:
            ordered = order.expressions if hasattr(order, 'expressions') else [order]
            for o in ordered:
                order_cols.append(o.sql(dialect=dialect))

        sig = f"{func_name}|{'|'.join(part_cols)}|{'|'.join(order_cols)}"
        if sig not in seen:
            seen.add(sig)
            result.append({
                "function": func_name,
                "partition_by": part_cols,
                "order_by": order_cols,
            })
    return result


def _extract_columns_per_source(node: exp.Expression, cte_names: set) -> dict:
    """
    Return {source_or_cte_name: [column_names]} for columns that have an
    explicit table qualifier (e.g. ro.customer_id -> raw_orders: [customer_id]).
    """
    # Build alias -> real name mapping from FROM + JOINs
    alias_map = {}
    for table in node.find_all(exp.Table):
        short = table.name
        full = _qualified_name(table)
        is_qualified = table.args.get("db") is not None
        real_name = full if is_qualified and short not in cte_names else short
        if real_name not in cte_names and is_qualified:
            real_name = full
        elif short in cte_names:
            real_name = short
        else:
            real_name = full

        alias_map[short] = real_name
        if table.alias:
            alias_map[table.alias] = real_name

    result = {}
    for col in node.find_all(exp.Column):
        table_ref = col.table
        col_name = col.name
        if table_ref and col_name:
            source = alias_map.get(table_ref, table_ref)
            if source not in result:
                result[source] = []
            if col_name not in result[source]:
                result[source].append(col_name)

    return result


def _estimate_complexity(node: exp.Expression, sql_text: str) -> dict:
    line_count = sql_text.count('\n') + 1
    expr_count = len(list(node.find_all(exp.Expression)))
    join_count = len(list(node.find_all(exp.Join)))
    subquery_count = len([s for s in node.find_all(exp.Subquery)
                          if not isinstance(s.parent, exp.CTE)])
    case_count = len(list(node.find_all(exp.Case)))
    window_count = len(list(node.find_all(exp.Window)))

    # Simple composite score
    score = (line_count * 0.5
             + join_count * 3
             + subquery_count * 5
             + case_count * 2
             + window_count * 2)
    if score < 10:
        level = "simple"
    elif score < 30:
        level = "moderate"
    elif score < 60:
        level = "complex"
    else:
        level = "very complex"

    return {
        "lines": line_count,
        "expressions": expr_count,
        "joins": join_count,
        "subqueries": subquery_count,
        "case_statements": case_count,
        "window_functions": window_count,
        "level": level,
    }


def _build_metadata(node: exp.Expression, cte_names: set, dialect: str) -> dict:
    """Extract all metadata from a SELECT expression."""
    sql_text = node.sql(dialect=dialect, pretty=True)
    return {
        "output_columns": _extract_output_columns(node),
        "tags": _extract_transformation_tags(node),
        "where": _extract_where_clause(node, dialect),
        "having": _extract_having_clause(node, dialect),
        "joins": _extract_joins(node, dialect),
        "group_by": _extract_group_by_keys(node, dialect),
        "window_functions": _extract_window_functions(node, dialect),
        "columns_per_source": _extract_columns_per_source(node, cte_names),
        "complexity": _estimate_complexity(node, sql_text),
    }


# ---------------------------------------------------------------------------
# Core parser
# ---------------------------------------------------------------------------

def parse_sql(sql: str, dialect: str = "snowflake") -> dict:
    """Parse SQL and return a graph dict with nodes, edges, and rich metadata."""
    parsed = sqlglot.parse_one(sql, dialect=dialect)

    cte_names = set()
    ctes_raw = {}

    with_clause = parsed.args.get("with_")
    if with_clause:
        for cte in with_clause.expressions:
            name = cte.alias
            cte_names.add(name)
            ctes_raw[name] = cte.this

    nodes = []
    edges = []
    source_tables = set()

    # CTE nodes + edges
    for name, body in ctes_raw.items():
        deps = _extract_dependencies(body, cte_names)
        meta = _build_metadata(body, cte_names, dialect)
        nodes.append({
            "id": name,
            "label": name,
            "type": "cte",
            "sql": body.sql(dialect=dialect, pretty=True),
            "deps": deps,
            "meta": meta,
        })
        for dep in deps:
            edges.append({"from": dep, "to": name})
            if dep not in cte_names:
                source_tables.add(dep)

    # Final SELECT
    final_node = parsed.copy()
    final_node.set("with_", None)
    final_deps = _extract_dependencies(final_node, cte_names)
    final_meta = _build_metadata(final_node, cte_names, dialect)

    nodes.append({
        "id": "__final__",
        "label": "Final SELECT",
        "type": "final",
        "sql": final_node.sql(dialect=dialect, pretty=True),
        "deps": final_deps,
        "meta": final_meta,
    })
    for dep in final_deps:
        edges.append({"from": dep, "to": "__final__"})
        if dep not in cte_names:
            source_tables.add(dep)

    # Source table nodes — find all columns referenced from each source across the whole query
    source_columns = {}
    for n in nodes:
        for src, cols in n["meta"].get("columns_per_source", {}).items():
            if src in source_tables:
                if src not in source_columns:
                    source_columns[src] = set()
                source_columns[src].update(cols)

    # Compute "used_by" for source tables
    source_used_by = {}
    for e in edges:
        if e["from"] in source_tables:
            if e["from"] not in source_used_by:
                source_used_by[e["from"]] = []
            source_used_by[e["from"]].append(e["to"])

    for t in sorted(source_tables):
        nodes.insert(0, {
            "id": t,
            "label": t,
            "type": "source",
            "sql": f"Source table: {t}",
            "deps": [],
            "meta": {
                "output_columns": [],
                "tags": [],
                "where": None,
                "having": None,
                "joins": [],
                "group_by": [],
                "window_functions": [],
                "columns_per_source": {},
                "complexity": None,
                "columns_referenced": sorted(source_columns.get(t, [])),
                "used_by": source_used_by.get(t, []),
            },
        })

    return {"nodes": nodes, "edges": edges}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print("Usage: python -m cteflow.parser <input.sql> [dialect] [-o output.json]")
        sys.exit(1)

    sql_file = Path(sys.argv[1])
    if not sql_file.exists():
        print(f"Error: file not found: {sql_file}")
        sys.exit(1)

    args = sys.argv[2:]
    dialect = "snowflake"
    output_path = None
    i = 0
    while i < len(args):
        if args[i] == "-o" and i + 1 < len(args):
            output_path = args[i + 1]
            i += 2
        else:
            dialect = args[i]
            i += 1

    sql = sql_file.read_text()
    graph = parse_sql(sql, dialect)

    if output_path is None:
        output_path = sql_file.stem + "_graph.json"

    Path(output_path).write_text(json.dumps(graph, indent=2))
    print(f"Parsed {len(graph['nodes'])} nodes, {len(graph['edges'])} edges -> {output_path}")


if __name__ == "__main__":
    main()
