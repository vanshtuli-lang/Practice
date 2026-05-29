"""
Tests for the credit_card_provider_flow_AF3 DAG.

Coverage:
  - DAG loads without import errors
  - Expected tasks are present and wired correctly
  - Income-tier routing logic (>$100k = premium)
  - Premium credit-limit calculation (15% of income)
  - Standard credit-limit calculation (8% of income, $2k floor)
  - Approval summary aggregation
  - Upsert skips gracefully when there are no portfolios
"""

from __future__ import annotations

import importlib
import sys
import types
from unittest.mock import MagicMock, patch
import pytest

DAG_ID = "credit_card_provider_flow_AF3"
DAG_FILE = "dags.credit_card_provider_flow_AF3"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ti(xcom_store: dict | None = None) -> MagicMock:
    """Return a mock TaskInstance whose xcom_pull reads from xcom_store."""
    store = xcom_store if xcom_store is not None else {}
    ti = MagicMock()
    ti.xcom_push.side_effect = lambda key, value: store.update({key: value})
    ti.xcom_pull.side_effect = lambda task_ids=None, key=None: store.get(key)
    return ti


def _make_context(xcom_store: dict | None = None) -> dict:
    return {"ti": _make_ti(xcom_store)}


# ---------------------------------------------------------------------------
# DAG integrity
# ---------------------------------------------------------------------------

class TestDagIntegrity:
    def test_dag_loads_without_errors(self):
        from airflow.models import DagBag
        bag = DagBag(include_examples=False)
        assert DAG_ID not in bag.import_errors, (
            f"DAG import error: {bag.import_errors.get(DAG_ID)}"
        )

    def test_dag_is_present(self):
        from airflow.models import DagBag
        bag = DagBag(include_examples=False)
        assert DAG_ID in bag.dags, f"{DAG_ID} not found in DagBag"

    def test_dag_has_tags(self):
        from airflow.models import DagBag
        bag = DagBag(include_examples=False)
        dag = bag.dags[DAG_ID]
        assert dag.tags, "DAG must have at least one tag"

    def test_expected_tasks_present(self):
        from airflow.models import DagBag
        bag = DagBag(include_examples=False)
        dag = bag.dags[DAG_ID]
        task_ids = {t.task_id for t in dag.tasks}
        expected = {
            "ingest_customer_data",
            "route_by_income_tier",
            "approve_premium_cards",
            "approve_standard_cards",
            "log_approval_summary",
            "upsert_to_snowflake",
        }
        assert expected == task_ids

    def test_ingest_feeds_branch(self):
        from airflow.models import DagBag
        bag = DagBag(include_examples=False)
        dag = bag.dags[DAG_ID]
        branch = dag.get_task("route_by_income_tier")
        upstream_ids = {t.task_id for t in branch.upstream_list}
        assert "ingest_customer_data" in upstream_ids

    def test_branch_feeds_approval_tasks(self):
        from airflow.models import DagBag
        bag = DagBag(include_examples=False)
        dag = bag.dags[DAG_ID]
        branch = dag.get_task("route_by_income_tier")
        downstream_ids = {t.task_id for t in branch.downstream_list}
        assert {"approve_premium_cards", "approve_standard_cards"} <= downstream_ids

    def test_summary_feeds_upsert(self):
        from airflow.models import DagBag
        bag = DagBag(include_examples=False)
        dag = bag.dags[DAG_ID]
        upsert = dag.get_task("upsert_to_snowflake")
        upstream_ids = {t.task_id for t in upsert.upstream_list}
        assert "log_approval_summary" in upstream_ids


# ---------------------------------------------------------------------------
# Business-logic tests (tasks invoked directly with mocked Airflow context)
# ---------------------------------------------------------------------------

# We import the DAG module once and grab the callable functions via a
# temporary DAG instantiation so we can unit-test the inner task functions.

@pytest.fixture(scope="module")
def task_fns():
    """
    Collect references to inner task callables by monkey-patching @task and @dag
    so that the real Python functions are captured before Airflow wraps them.

    Strategy:
      - fake_task: records fn.__name__ → fn in `captured`, then returns a
        MagicMock wrapper so that the DAG-body wiring calls (e.g. branch >> [...])
        don't raise.
      - fake_dag: accepts the decorator kwargs (dag_id, schedule, …), then returns
        a decorator that calls the DAG body function once (to populate `captured`)
        and returns the original function so the module-level
        `credit_card_provider_flow_AF3()` call also works.
    """
    captured: dict[str, object] = {}

    def fake_task(*args, **kwargs):
        """Handles both @task and @task(...) forms."""
        def decorator(fn):
            captured[fn.__name__] = fn
            stub = MagicMock(name=fn.__name__)
            stub.__name__ = fn.__name__
            return stub
        if args and callable(args[0]):
            # bare @task (no parens)
            return decorator(args[0])
        return decorator

    fake_task.branch = fake_task  # handle @task.branch

    def fake_dag(*args, **kwargs):
        """Handles @dag(dag_id=..., ...) — returns a decorator."""
        def decorator(fn):
            fn()   # execute the body to register all inner @task functions
            return fn
        return decorator

    fake_trigger_rule = MagicMock()
    fake_trigger_rule.NONE_FAILED_MIN_ONE_SUCCESS = "none_failed_min_one_success"

    stub_providers: dict[str, types.ModuleType] = {}
    for mod_path in [
        "airflow.providers.amazon",
        "airflow.providers.amazon.aws",
        "airflow.providers.amazon.aws.hooks",
        "airflow.providers.amazon.aws.hooks.s3",
        "airflow.providers.snowflake",
        "airflow.providers.snowflake.hooks",
        "airflow.providers.snowflake.hooks.snowflake",
    ]:
        stub_providers[mod_path] = types.ModuleType(mod_path)

    airflow_sdk_stub = types.ModuleType("airflow.sdk")
    airflow_sdk_stub.dag = fake_dag
    airflow_sdk_stub.task = fake_task

    trigger_rule_stub = types.ModuleType("airflow.task.trigger_rule")
    trigger_rule_stub.TriggerRule = fake_trigger_rule

    overrides = {
        "airflow.sdk": airflow_sdk_stub,
        "airflow.task.trigger_rule": trigger_rule_stub,
        **stub_providers,
    }

    for key in list(sys.modules.keys()):
        if "credit_card_provider_flow_AF3" in key:
            del sys.modules[key]

    with patch.dict(sys.modules, overrides):
        import dags.credit_card_provider_flow_AF3  # noqa: F401

    return captured


class TestRoutingLogic:
    CUSTOMERS = [
        {"application_id": "A001", "customer_name": "Alice", "annual_income": 150_000},
        {"application_id": "A002", "customer_name": "Bob",   "annual_income":  80_000},
        {"application_id": "A003", "customer_name": "Carol", "annual_income": 100_000},  # boundary — standard
        {"application_id": "A004", "customer_name": "Dave",  "annual_income": 100_001},  # boundary — premium
    ]

    def test_splits_premium_and_standard(self, task_fns):
        store = {}
        ctx = _make_context(store)
        result = task_fns["route_by_income_tier"](self.CUSTOMERS, **ctx)
        assert "approve_premium_cards" in result
        assert "approve_standard_cards" in result
        assert len(store["premium_customers"]) == 2   # Alice + Dave
        assert len(store["standard_customers"]) == 2  # Bob + Carol

    def test_boundary_100k_is_standard(self, task_fns):
        customers = [{"application_id": "X", "customer_name": "X", "annual_income": 100_000}]
        store = {}
        ctx = _make_context(store)
        task_fns["route_by_income_tier"](customers, **ctx)
        assert len(store["standard_customers"]) == 1
        assert len(store["premium_customers"]) == 0

    def test_returns_only_premium_branch_when_no_standard(self, task_fns):
        customers = [{"application_id": "X", "customer_name": "X", "annual_income": 200_000}]
        store = {}
        ctx = _make_context(store)
        result = task_fns["route_by_income_tier"](customers, **ctx)
        assert result == ["approve_premium_cards"]

    def test_returns_only_standard_branch_when_no_premium(self, task_fns):
        customers = [{"application_id": "X", "customer_name": "X", "annual_income": 50_000}]
        store = {}
        ctx = _make_context(store)
        result = task_fns["route_by_income_tier"](customers, **ctx)
        assert result == ["approve_standard_cards"]


class TestPremiumApproval:
    def test_credit_limit_is_15_percent(self, task_fns):
        store = {"premium_customers": [
            {"application_id": "P1", "customer_name": "Alice", "annual_income": 200_000},
        ]}
        ctx = _make_context(store)
        portfolio = task_fns["approve_premium_cards"](**ctx)
        assert len(portfolio) == 1
        assert portfolio[0]["credit_limit"] == 30_000.0
        assert portfolio[0]["card_product"] == "Infinite Sapphire"
        assert portfolio[0]["income_tier"] == "PREMIUM"

    def test_empty_premium_list_returns_empty(self, task_fns):
        store = {"premium_customers": []}
        ctx = _make_context(store)
        portfolio = task_fns["approve_premium_cards"](**ctx)
        assert portfolio == []

    def test_none_xcom_treated_as_empty(self, task_fns):
        store = {}  # xcom_pull returns None
        ctx = _make_context(store)
        portfolio = task_fns["approve_premium_cards"](**ctx)
        assert portfolio == []


class TestStandardApproval:
    def test_credit_limit_is_8_percent(self, task_fns):
        store = {"standard_customers": [
            {"application_id": "S1", "customer_name": "Bob", "annual_income": 50_000},
        ]}
        ctx = _make_context(store)
        portfolio = task_fns["approve_standard_cards"](**ctx)
        assert portfolio[0]["credit_limit"] == 4_000.0
        assert portfolio[0]["card_product"] == "Classic Rewards"
        assert portfolio[0]["income_tier"] == "STANDARD"

    def test_floor_applied_below_25k(self, task_fns):
        # 8% of 20,000 = 1,600 → floor kicks in → 2,000
        store = {"standard_customers": [
            {"application_id": "S2", "customer_name": "Carol", "annual_income": 20_000},
        ]}
        ctx = _make_context(store)
        portfolio = task_fns["approve_standard_cards"](**ctx)
        assert portfolio[0]["credit_limit"] == 2_000.0

    def test_floor_boundary_exactly_25k(self, task_fns):
        # 8% of 25,000 = 2,000 — exactly the floor, no adjustment needed
        store = {"standard_customers": [
            {"application_id": "S3", "customer_name": "Dave", "annual_income": 25_000},
        ]}
        ctx = _make_context(store)
        portfolio = task_fns["approve_standard_cards"](**ctx)
        assert portfolio[0]["credit_limit"] == 2_000.0


class TestApprovalSummary:
    def test_aggregates_both_portfolios(self, task_fns):
        premium  = [{"application_id": "P1", "customer_name": "A", "annual_income": 200_000, "card_product": "Infinite Sapphire", "credit_limit": 30_000.0, "income_tier": "PREMIUM"}]
        standard = [{"application_id": "S1", "customer_name": "B", "annual_income": 50_000,  "card_product": "Classic Rewards",   "credit_limit":  4_000.0, "income_tier": "STANDARD"}]
        summary = task_fns["log_approval_summary"](premium_portfolio=premium, standard_portfolio=standard)
        assert summary["total_accounts"] == 2
        assert summary["total_credit_volume"] == 34_000.0
        assert summary["avg_credit_limit"] == 17_000.0

    def test_handles_missing_premium_portfolio(self, task_fns):
        standard = [{"application_id": "S1", "customer_name": "B", "annual_income": 50_000, "card_product": "Classic Rewards", "credit_limit": 4_000.0, "income_tier": "STANDARD"}]
        summary = task_fns["log_approval_summary"](premium_portfolio=None, standard_portfolio=standard)
        assert summary["total_accounts"] == 1

    def test_handles_both_none(self, task_fns):
        summary = task_fns["log_approval_summary"](premium_portfolio=None, standard_portfolio=None)
        assert summary["total_accounts"] == 0
        assert summary["total_credit_volume"] == 0.0


class TestUpsertToSnowflake:
    def test_skips_when_no_portfolios(self, task_fns, capsys):
        summary = {"approved_portfolios": []}

        snowflake_hook_mock = MagicMock()
        with patch.dict(sys.modules, {
            "airflow.providers.snowflake.hooks.snowflake": MagicMock(SnowflakeHook=MagicMock(return_value=snowflake_hook_mock))
        }):
            task_fns["upsert_to_snowflake"](summary)

        snowflake_hook_mock.run.assert_not_called()

    def test_calls_hook_run_with_all_records(self, task_fns):
        portfolios = [
            {"application_id": "P1", "customer_name": "Alice", "annual_income": 200_000, "card_product": "Infinite Sapphire", "credit_limit": 30_000.0, "income_tier": "PREMIUM"},
            {"application_id": "S1", "customer_name": "Bob",   "annual_income":  50_000, "card_product": "Classic Rewards",   "credit_limit":  4_000.0, "income_tier": "STANDARD"},
        ]
        summary = {"approved_portfolios": portfolios}

        hook_instance = MagicMock()
        with patch.dict(sys.modules, {
            "airflow.providers.snowflake.hooks.snowflake": MagicMock(SnowflakeHook=MagicMock(return_value=hook_instance))
        }):
            task_fns["upsert_to_snowflake"](summary)

        hook_instance.run.assert_called_once()
        call = hook_instance.run.call_args
        sql = call.args[0]
        params = call.kwargs.get("parameters") or call.args[1]
        assert "MERGE INTO SANDBOX.VANSHTULI.CREDIT_ACCOUNTS" in sql
        assert len(params) == 12  # 2 rows × 6 cols
