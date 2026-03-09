"""Unit tests for cteflow.parser."""

import pytest
from pathlib import Path

import cteflow.parser
import sqlglot
from sqlglot import exp

EXAMPLES_DIR = Path(__file__).resolve().parent.parent / "examples"
SAMPLE_SQL = (EXAMPLES_DIR / "sample_query.sql").read_text()
COMPLEX_SQL = (EXAMPLES_DIR / "complex_query.sql").read_text()


# ---------------------------------------------------------------------------
# _qualified_name
# ---------------------------------------------------------------------------

class TestQualifiedName:
    def test_simple_table(self):
        table = sqlglot.parse_one("SELECT * FROM orders", dialect="snowflake").find(exp.Table)
        assert cteflow.parser._qualified_name(table) == "orders"

    def test_schema_qualified(self):
        table = sqlglot.parse_one("SELECT * FROM warehouse.orders", dialect="snowflake").find(exp.Table)
        assert cteflow.parser._qualified_name(table) == "warehouse.orders"

    def test_catalog_schema_qualified(self):
        table = sqlglot.parse_one("SELECT * FROM mydb.warehouse.orders", dialect="snowflake").find(exp.Table)
        assert cteflow.parser._qualified_name(table) == "mydb.warehouse.orders"


# ---------------------------------------------------------------------------
# _extract_dependencies
# ---------------------------------------------------------------------------

class TestExtractDependencies:
    def test_cte_dependency(self):
        sql = "SELECT * FROM raw_orders WHERE status = 'ok'"
        node = sqlglot.parse_one(sql, dialect="snowflake")
        deps = cteflow.parser._extract_dependencies(node, {"raw_orders", "other_cte"})
        assert "raw_orders" in deps

    def test_source_table_dependency(self):
        sql = "SELECT * FROM warehouse.orders"
        node = sqlglot.parse_one(sql, dialect="snowflake")
        deps = cteflow.parser._extract_dependencies(node, set())
        assert "warehouse.orders" in deps

    def test_mixed_dependencies(self):
        sql = "SELECT a.*, b.name FROM raw_orders a JOIN warehouse.customers b ON a.id = b.id"
        node = sqlglot.parse_one(sql, dialect="snowflake")
        deps = cteflow.parser._extract_dependencies(node, {"raw_orders"})
        assert "raw_orders" in deps
        assert "warehouse.customers" in deps

    def test_qualified_table_not_treated_as_cte(self):
        """A qualified table like warehouse.order_items should not match CTE name order_items."""
        sql = "SELECT * FROM warehouse.order_items"
        node = sqlglot.parse_one(sql, dialect="snowflake")
        deps = cteflow.parser._extract_dependencies(node, {"order_items"})
        assert "warehouse.order_items" in deps


# ---------------------------------------------------------------------------
# _extract_output_columns
# ---------------------------------------------------------------------------

class TestExtractOutputColumns:
    def test_named_columns(self):
        node = sqlglot.parse_one("SELECT order_id, customer_id, amount FROM t", dialect="snowflake")
        cols = cteflow.parser._extract_output_columns(node)
        assert cols == ["order_id", "customer_id", "amount"]

    def test_aliased_columns(self):
        node = sqlglot.parse_one("SELECT SUM(amount) AS total_revenue, COUNT(*) AS order_count FROM t", dialect="snowflake")
        cols = cteflow.parser._extract_output_columns(node)
        assert "total_revenue" in cols
        assert "order_count" in cols

    def test_star(self):
        node = sqlglot.parse_one("SELECT * FROM t", dialect="snowflake")
        cols = cteflow.parser._extract_output_columns(node)
        assert cols == ["*"]

    def test_table_star(self):
        node = sqlglot.parse_one("SELECT ct.*, c.name FROM ct JOIN c ON ct.id = c.id", dialect="snowflake")
        cols = cteflow.parser._extract_output_columns(node)
        # sqlglot represents ct.* as a plain Star (no table qualifier preserved in some versions)
        assert "*" in cols or "ct.*" in cols
        assert "name" in cols


# ---------------------------------------------------------------------------
# _extract_transformation_tags
# ---------------------------------------------------------------------------

class TestExtractTransformationTags:
    def test_group_by(self):
        node = sqlglot.parse_one("SELECT customer_id, SUM(amount) FROM t GROUP BY customer_id", dialect="snowflake")
        tags = cteflow.parser._extract_transformation_tags(node)
        assert "GROUP BY" in tags

    def test_filter(self):
        node = sqlglot.parse_one("SELECT * FROM t WHERE status = 'active'", dialect="snowflake")
        tags = cteflow.parser._extract_transformation_tags(node)
        assert "FILTER" in tags

    def test_join(self):
        node = sqlglot.parse_one("SELECT * FROM a JOIN b ON a.id = b.id", dialect="snowflake")
        tags = cteflow.parser._extract_transformation_tags(node)
        assert any("JOIN" in t for t in tags)

    def test_left_join(self):
        node = sqlglot.parse_one("SELECT * FROM a LEFT JOIN b ON a.id = b.id", dialect="snowflake")
        tags = cteflow.parser._extract_transformation_tags(node)
        assert any("LEFT" in t for t in tags)

    def test_window(self):
        node = sqlglot.parse_one("SELECT ROW_NUMBER() OVER (PARTITION BY id ORDER BY dt) AS rn FROM t", dialect="snowflake")
        tags = cteflow.parser._extract_transformation_tags(node)
        assert "WINDOW" in tags

    def test_distinct(self):
        node = sqlglot.parse_one("SELECT DISTINCT category FROM t", dialect="snowflake")
        tags = cteflow.parser._extract_transformation_tags(node)
        assert "DISTINCT" in tags

    def test_having(self):
        node = sqlglot.parse_one("SELECT customer_id, COUNT(*) AS cnt FROM t GROUP BY customer_id HAVING COUNT(*) > 5", dialect="snowflake")
        tags = cteflow.parser._extract_transformation_tags(node)
        assert "HAVING" in tags

    def test_order_by(self):
        node = sqlglot.parse_one("SELECT * FROM t ORDER BY created_at DESC", dialect="snowflake")
        tags = cteflow.parser._extract_transformation_tags(node)
        assert "ORDER BY" in tags

    def test_limit(self):
        node = sqlglot.parse_one("SELECT * FROM t LIMIT 10", dialect="snowflake")
        tags = cteflow.parser._extract_transformation_tags(node)
        assert "LIMIT" in tags

    def test_no_tags_for_simple_select(self):
        node = sqlglot.parse_one("SELECT * FROM t", dialect="snowflake")
        tags = cteflow.parser._extract_transformation_tags(node)
        assert tags == []


# ---------------------------------------------------------------------------
# _extract_where_clause / _extract_having_clause
# ---------------------------------------------------------------------------

class TestExtractClauses:
    def test_where_present(self):
        node = sqlglot.parse_one("SELECT * FROM t WHERE status = 'active'", dialect="snowflake")
        w = cteflow.parser._extract_where_clause(node, "snowflake")
        assert w is not None
        assert "status" in w

    def test_where_absent(self):
        node = sqlglot.parse_one("SELECT * FROM t", dialect="snowflake")
        assert cteflow.parser._extract_where_clause(node, "snowflake") is None

    def test_having_present(self):
        node = sqlglot.parse_one("SELECT id, COUNT(*) FROM t GROUP BY id HAVING COUNT(*) > 1", dialect="snowflake")
        h = cteflow.parser._extract_having_clause(node, "snowflake")
        assert h is not None
        assert "COUNT" in h.upper()

    def test_having_absent(self):
        node = sqlglot.parse_one("SELECT * FROM t", dialect="snowflake")
        assert cteflow.parser._extract_having_clause(node, "snowflake") is None


# ---------------------------------------------------------------------------
# _extract_joins
# ---------------------------------------------------------------------------

class TestExtractJoins:
    def test_inner_join(self):
        node = sqlglot.parse_one("SELECT * FROM a JOIN b ON a.id = b.id", dialect="snowflake")
        joins = cteflow.parser._extract_joins(node, "snowflake")
        assert len(joins) == 1
        assert joins[0]["table"] == "b"
        assert joins[0]["on"] != ""

    def test_left_join_with_alias(self):
        node = sqlglot.parse_one("SELECT * FROM a LEFT JOIN warehouse.customers c ON a.id = c.id", dialect="snowflake")
        joins = cteflow.parser._extract_joins(node, "snowflake")
        assert len(joins) == 1
        assert "LEFT" in joins[0]["type"]
        assert joins[0]["table"] == "warehouse.customers"
        assert joins[0]["alias"] == "c"

    def test_multiple_joins(self):
        sql = "SELECT * FROM a JOIN b ON a.id = b.id LEFT JOIN c ON a.id = c.id"
        node = sqlglot.parse_one(sql, dialect="snowflake")
        joins = cteflow.parser._extract_joins(node, "snowflake")
        assert len(joins) == 2

    def test_no_joins(self):
        node = sqlglot.parse_one("SELECT * FROM t", dialect="snowflake")
        joins = cteflow.parser._extract_joins(node, "snowflake")
        assert joins == []


# ---------------------------------------------------------------------------
# _extract_group_by_keys
# ---------------------------------------------------------------------------

class TestExtractGroupByKeys:
    def test_single_key(self):
        node = sqlglot.parse_one("SELECT customer_id, SUM(amount) FROM t GROUP BY customer_id", dialect="snowflake")
        keys = cteflow.parser._extract_group_by_keys(node, "snowflake")
        assert "customer_id" in keys

    def test_multiple_keys(self):
        node = sqlglot.parse_one("SELECT a, b, COUNT(*) FROM t GROUP BY a, b", dialect="snowflake")
        keys = cteflow.parser._extract_group_by_keys(node, "snowflake")
        assert len(keys) == 2

    def test_no_group_by(self):
        node = sqlglot.parse_one("SELECT * FROM t", dialect="snowflake")
        keys = cteflow.parser._extract_group_by_keys(node, "snowflake")
        assert keys == []


# ---------------------------------------------------------------------------
# _extract_window_functions
# ---------------------------------------------------------------------------

class TestExtractWindowFunctions:
    def test_row_number(self):
        sql = "SELECT ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS rn FROM t"
        node = sqlglot.parse_one(sql, dialect="snowflake")
        wins = cteflow.parser._extract_window_functions(node, "snowflake")
        assert len(wins) >= 1
        assert wins[0]["partition_by"] == ["customer_id"]
        assert len(wins[0]["order_by"]) >= 1

    def test_multiple_windows(self):
        sql = """SELECT
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY dt) AS rn,
            LAG(dt) OVER (PARTITION BY id ORDER BY dt) AS prev_dt
        FROM t"""
        node = sqlglot.parse_one(sql, dialect="snowflake")
        wins = cteflow.parser._extract_window_functions(node, "snowflake")
        assert len(wins) == 2

    def test_no_window(self):
        node = sqlglot.parse_one("SELECT * FROM t", dialect="snowflake")
        wins = cteflow.parser._extract_window_functions(node, "snowflake")
        assert wins == []


# ---------------------------------------------------------------------------
# _extract_columns_per_source
# ---------------------------------------------------------------------------

class TestExtractColumnsPerSource:
    def test_qualified_columns(self):
        sql = "SELECT a.order_id, a.amount, b.name FROM raw_orders a JOIN customers b ON a.id = b.id"
        node = sqlglot.parse_one(sql, dialect="snowflake")
        cps = cteflow.parser._extract_columns_per_source(node, {"raw_orders"})
        assert "raw_orders" in cps
        assert "order_id" in cps["raw_orders"]
        assert "amount" in cps["raw_orders"]

    def test_unqualified_columns_not_included(self):
        sql = "SELECT order_id FROM t"
        node = sqlglot.parse_one(sql, dialect="snowflake")
        cps = cteflow.parser._extract_columns_per_source(node, set())
        # No table qualifier on order_id, so it shouldn't appear
        assert len(cps) == 0


# ---------------------------------------------------------------------------
# _estimate_complexity
# ---------------------------------------------------------------------------

class TestEstimateComplexity:
    def test_simple_query(self):
        sql = "SELECT * FROM t"
        node = sqlglot.parse_one(sql, dialect="snowflake")
        c = cteflow.parser._estimate_complexity(node, sql)
        assert c["level"] == "simple"
        assert c["joins"] == 0
        assert c["window_functions"] == 0

    def test_complex_query(self):
        sql = """SELECT a.*, b.name,
            ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY a.dt) AS rn,
            CASE WHEN a.status = 'x' THEN 1 ELSE 0 END AS flag
        FROM t a
        JOIN u b ON a.id = b.id
        LEFT JOIN v c ON a.id = c.id
        WHERE a.dt > '2024-01-01'
        GROUP BY a.id"""
        node = sqlglot.parse_one(sql, dialect="snowflake")
        c = cteflow.parser._estimate_complexity(node, sql)
        assert c["joins"] == 2
        assert c["window_functions"] == 1
        assert c["case_statements"] == 1
        assert c["level"] in ("moderate", "complex", "very complex")


# ---------------------------------------------------------------------------
# parse_sql — integration tests with sample queries
# ---------------------------------------------------------------------------

class TestParseSqlSample:
    """Tests against examples/sample_query.sql (4 CTEs)."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.graph = cteflow.parser.parse_sql(SAMPLE_SQL, dialect="snowflake")

    def test_node_count(self):
        # 2 source tables + 4 CTEs + 1 final = 7
        assert len(self.graph["nodes"]) == 7

    def test_edge_count(self):
        assert len(self.graph["edges"]) > 0

    def test_node_types(self):
        types = {n["type"] for n in self.graph["nodes"]}
        assert types == {"source", "cte", "final"}

    def test_cte_names(self):
        cte_names = {n["id"] for n in self.graph["nodes"] if n["type"] == "cte"}
        assert cte_names == {"raw_orders", "valid_orders", "customer_totals", "enriched_customers"}

    def test_source_tables(self):
        sources = {n["id"] for n in self.graph["nodes"] if n["type"] == "source"}
        assert "warehouse.orders" in sources
        assert "warehouse.customers" in sources

    def test_final_node_exists(self):
        final = [n for n in self.graph["nodes"] if n["type"] == "final"]
        assert len(final) == 1
        assert final[0]["id"] == "__final__"

    def test_edge_chain(self):
        edges = {(e["from"], e["to"]) for e in self.graph["edges"]}
        assert ("warehouse.orders", "raw_orders") in edges
        assert ("raw_orders", "valid_orders") in edges
        assert ("valid_orders", "customer_totals") in edges
        assert ("customer_totals", "enriched_customers") in edges
        assert ("enriched_customers", "__final__") in edges

    def test_customer_totals_has_group_by(self):
        ct = next(n for n in self.graph["nodes"] if n["id"] == "customer_totals")
        assert "GROUP BY" in ct["meta"]["tags"]
        assert "customer_id" in ct["meta"]["group_by"]

    def test_enriched_customers_has_join(self):
        ec = next(n for n in self.graph["nodes"] if n["id"] == "enriched_customers")
        assert any("JOIN" in t for t in ec["meta"]["tags"])
        assert len(ec["meta"]["joins"]) == 1

    def test_raw_orders_has_filter(self):
        ro = next(n for n in self.graph["nodes"] if n["id"] == "raw_orders")
        assert "FILTER" in ro["meta"]["tags"]
        assert ro["meta"]["where"] is not None

    def test_source_node_has_columns_referenced(self):
        src = next(n for n in self.graph["nodes"] if n["id"] == "warehouse.orders")
        assert "columns_referenced" in src["meta"]

    def test_source_node_has_used_by(self):
        src = next(n for n in self.graph["nodes"] if n["id"] == "warehouse.orders")
        assert "used_by" in src["meta"]
        assert "raw_orders" in src["meta"]["used_by"]

    def test_all_nodes_have_sql(self):
        for node in self.graph["nodes"]:
            assert "sql" in node and node["sql"]

    def test_all_cte_nodes_have_metadata(self):
        for node in self.graph["nodes"]:
            if node["type"] == "cte":
                assert "meta" in node
                assert "output_columns" in node["meta"]
                assert "tags" in node["meta"]
                assert "complexity" in node["meta"]


class TestParseSqlComplex:
    """Tests against examples/complex_query.sql (16 CTEs)."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.graph = cteflow.parser.parse_sql(COMPLEX_SQL, dialect="snowflake")

    def test_node_count(self):
        # 5 source tables + 16 CTEs + 1 final = 22
        assert len(self.graph["nodes"]) == 22

    def test_edge_count(self):
        assert len(self.graph["edges"]) == 31

    def test_all_16_ctes(self):
        cte_names = {n["id"] for n in self.graph["nodes"] if n["type"] == "cte"}
        expected = {
            "raw_orders", "order_items", "enriched_orders",
            "customer_first_order", "customer_order_sequences",
            "customer_lifetime_stats", "customer_segments",
            "cohort_retention", "cohort_sizes", "cohort_retention_rates",
            "product_affinity_pairs", "top_product_pairs",
            "supplier_performance", "fraud_risk_signals",
            "fraud_scored_orders", "monthly_exec_summary",
        }
        assert cte_names == expected

    def test_source_tables(self):
        sources = {n["id"] for n in self.graph["nodes"] if n["type"] == "source"}
        expected = {
            "warehouse.orders", "warehouse.order_items",
            "warehouse.products", "warehouse.customers",
            "warehouse.suppliers",
        }
        assert sources == expected

    def test_customer_segments_has_window(self):
        cs = next(n for n in self.graph["nodes"] if n["id"] == "customer_segments")
        assert "WINDOW" in cs["meta"]["tags"]
        assert len(cs["meta"]["window_functions"]) >= 1

    def test_fraud_scored_orders_has_case(self):
        fso = next(n for n in self.graph["nodes"] if n["id"] == "fraud_scored_orders")
        assert fso["meta"]["complexity"]["case_statements"] >= 1

    def test_monthly_exec_summary_has_group_by(self):
        mes = next(n for n in self.graph["nodes"] if n["id"] == "monthly_exec_summary")
        assert "GROUP BY" in mes["meta"]["tags"]

    def test_no_self_referencing_edges(self):
        for edge in self.graph["edges"]:
            assert edge["from"] != edge["to"]

    def test_source_tables_have_no_deps(self):
        for node in self.graph["nodes"]:
            if node["type"] == "source":
                assert node["deps"] == []


# ---------------------------------------------------------------------------
# parse_sql — edge cases
# ---------------------------------------------------------------------------

class TestParseSqlEdgeCases:
    def test_no_ctes(self):
        sql = "SELECT * FROM warehouse.orders WHERE status = 'active'"
        graph = cteflow.parser.parse_sql(sql, dialect="snowflake")
        assert len([n for n in graph["nodes"] if n["type"] == "final"]) == 1
        assert len([n for n in graph["nodes"] if n["type"] == "source"]) == 1
        assert len([n for n in graph["nodes"] if n["type"] == "cte"]) == 0

    def test_single_cte(self):
        sql = "WITH a AS (SELECT 1 AS x) SELECT * FROM a"
        graph = cteflow.parser.parse_sql(sql, dialect="snowflake")
        ctes = [n for n in graph["nodes"] if n["type"] == "cte"]
        assert len(ctes) == 1
        assert ctes[0]["id"] == "a"

    def test_different_dialect(self):
        sql = "WITH a AS (SELECT 1 AS x) SELECT * FROM a"
        graph = cteflow.parser.parse_sql(sql, dialect="postgres")
        assert len(graph["nodes"]) >= 2

    def test_union_detected(self):
        sql = """WITH combined AS (
            SELECT id FROM a
            UNION ALL
            SELECT id FROM b
        ) SELECT * FROM combined"""
        graph = cteflow.parser.parse_sql(sql, dialect="snowflake")
        cte = next(n for n in graph["nodes"] if n["id"] == "combined")
        assert "UNION" in cte["meta"]["tags"]
