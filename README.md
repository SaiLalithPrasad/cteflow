# cteflow

Visualize SQL CTE data flows as interactive, self-contained HTML graphs. Runs entirely locally — no cloud services, no data leaves your machine.

## Quick Start

```bash
# 1. Clone and set up
git clone <repo-url>
cd view-vis
python3 -m venv .venv
source .venv/bin/activate        # macOS/Linux
# .venv\Scripts\activate         # Windows

# 2. Install
pip install -e .

# 3. Run
cteflow examples/sample_query.sql
```

That's it. A browser tab opens with your interactive flow graph.

## Usage

```bash
# Default dialect is Snowflake
cteflow my_query.sql

# Specify a dialect
cteflow my_query.sql postgres
cteflow my_query.sql bigquery

# Also works as a Python module (no install needed from project root)
python -m cteflow examples/complex_query.sql
```

### Supported SQL Dialects

`snowflake` (default), `postgres`, `bigquery`, `mysql`, `spark`, `tsql`, `duckdb`, `hive`, `presto`, `redshift`, `sqlite`, `trino`

### Output

- Generates `output/<filename>_flow.html` and auto-opens it in your browser
- The HTML file is fully self-contained — share it with anyone, no server needed

## What You Get

### Interactive Graph
- **Source tables** (green) flow into **CTEs** (blue) which feed the **Final SELECT** (orange)
- Toggle between **top-down** and **left-right** layout
- **Drag** any node to rearrange
- **Hover** a node to highlight its full upstream/downstream path
- **Click** a node to open the detail panel

### Detail Panel (click any node)
When you click a node, a panel slides in showing:

| Section | Description |
|---------|-------------|
| Graph Context | Direct inputs, total upstream, direct outputs, total downstream |
| Transformations | Auto-detected: GROUP BY, JOIN types, WINDOW, FILTER, DISTINCT, etc. |
| Complexity | Color-coded badge: simple / moderate / complex / very complex |
| Dependencies | What this node reads from |
| Output Columns | Columns produced by this CTE/SELECT |
| Joins | Type, table, alias, ON condition for each JOIN |
| WHERE / HAVING | Filter clauses shown separately |
| GROUP BY | Aggregation keys |
| Window Functions | Function name, PARTITION BY, ORDER BY |
| Columns Per Source | Which columns are used from each upstream table |
| Complexity Breakdown | Lines, joins, windows, CASE statements, subqueries, AST nodes |
| Full SQL | Syntax-highlighted source with keyword/function/string/number coloring |

For **source table** nodes, the panel shows:
- **Columns Referenced** — every column used from this table across the entire query
- **Used By** — which CTEs reference this table

## Project Structure

```
view-vis/
├── cteflow/              # Python package
│   ├── __init__.py       # Version + public API (parse_sql, generate_html)
│   ├── __main__.py       # python -m cteflow support
│   ├── parser.py         # SQL parsing + metadata extraction
│   ├── renderer.py       # HTML generation (self-contained template)
│   └── cli.py            # CLI entry point
├── examples/
│   ├── sample_query.sql  # Simple 4-CTE query
│   └── complex_query.sql # 16-CTE e-commerce analytics pipeline
├── output/               # Generated HTML files
├── pyproject.toml        # Package configuration
├── LICENSE               # MIT
└── README.md
```

## Programmatic Use

```python
from cteflow import parse_sql, generate_html

sql = open("my_query.sql").read()
graph = parse_sql(sql, dialect="snowflake")  # returns dict with nodes + edges
html = generate_html(graph, "my_query.sql")  # returns self-contained HTML string

# Write it yourself, or just use the CLI
with open("my_flow.html", "w") as f:
    f.write(html)
```

## Example

### Input

```sql
WITH raw_orders AS (
    SELECT order_id, customer_id, amount, status, order_date
    FROM warehouse.orders
    WHERE order_date >= '2024-01-01'
),
valid_orders AS (
    SELECT * FROM raw_orders WHERE status IN ('completed', 'shipped')
),
customer_totals AS (
    SELECT customer_id, SUM(amount) AS total_revenue, COUNT(*) AS order_count
    FROM valid_orders GROUP BY customer_id
),
enriched_customers AS (
    SELECT ct.*, c.name, c.segment
    FROM customer_totals ct
    JOIN warehouse.customers c ON ct.customer_id = c.customer_id
)
SELECT * FROM enriched_customers WHERE total_revenue > 1000
```

### Output

```
warehouse.orders --> raw_orders --> valid_orders --> customer_totals --> enriched_customers --> Final SELECT
                                                                               ^
warehouse.customers -----------------------------------------------------------+
```

The complex example (`examples/complex_query.sql`) produces a graph with 22 nodes and 31 edges across 5 analytical domains: order enrichment, customer lifecycle, cohort analysis, product affinity, and fraud detection.

## Requirements

- Python 3.10+
- sqlglot >= 26.0.0 (installed automatically)

## License

MIT
