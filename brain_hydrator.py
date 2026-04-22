from __future__ import annotations

from typing import Any, Dict, List

from brain_schema import BrainNodeType


class BrainHydrator:
    """Hydrates user-scoped long-term memory references from QBRAIN tables."""

    def __init__(self, qb: Any):
        self.qb = qb

    def _iter_manager_tables(self) -> List[Dict[str, Any]]:
        tables: List[Dict[str, Any]] = []
        managers_info = self.qb.get_managers_info() if hasattr(self.qb, "get_managers_info") else []
        for manager in managers_info:
            default_table = manager.get("default_table")
            default_schema = manager.get("schema")
            if default_table and isinstance(default_schema, dict):
                tables.append({"table_name": default_table, "schema": default_schema})

            for additional in manager.get("additional_tables") or []:
                tname = additional.get("table_name")
                schema = additional.get("schema")
                if tname and isinstance(schema, dict):
                    tables.append({"table_name": tname, "schema": schema})
        return tables

    def hydrate_user_long_term(self, user_id: str) -> List[Dict[str, Any]]:
        print("hydrate_user_long_term...")
        nodes: List[Dict[str, Any]] = []
        tables = self._iter_manager_tables()

        for table_entry in tables:
            table_name = table_entry["table_name"]
            schema = table_entry["schema"]
            if "user_id" not in schema:
                continue

            select_fields = ["id", "user_id"]
            if "description" in schema:
                select_fields.append("description")
            if "updated_at" in schema:
                select_fields.append("updated_at")
            if "status" in schema:
                select_fields.append("status")
            select_sql = ", ".join(select_fields)

            ref_table = self.qb._table_ref(table_name) if hasattr(self.qb, "_table_ref") else table_name
            sql = (
                f"SELECT {select_sql} FROM {ref_table} "
                "WHERE user_id = @user_id "
                "ORDER BY updated_at DESC"
            )
            try:
                rows = self.qb.run_query(sql, params={"user_id": user_id}, conv_to_dict=True) or []
            except Exception:
                # Some tables may not have updated_at for order by in old schemas; retry without order.
                sql = f"SELECT {select_sql} FROM {ref_table} WHERE user_id = @user_id"
                rows = self.qb.run_query(sql, params={"user_id": user_id}, conv_to_dict=True) or []

            for row in rows:
                row_id = row.get("id")
                if not row_id:
                    continue
                node = {
                    "id": f"LTS::{table_name}::{row_id}",
                    "type": BrainNodeType.LONG_TERM_STORAGE,
                    "table_name": table_name,
                    "row_id": str(row_id),
                    "user_id": str(user_id),
                    "description": str(row.get("description") or ""),
                    "status": str(row.get("status") or ""),
                    "updated_at": str(row.get("updated_at") or ""),
                }
                nodes.append(node)

        print("hydrate_user_long_term... done")
        return nodes


if __name__ == "__main__":
    # Minimal workflow: hydrate with stub qb (no get_managers_info -> empty)
    class _StubQb:
        def get_managers_info(self):
            return []

    h = BrainHydrator(qb=_StubQb())
    out = h.hydrate_user_long_term("public")
    assert out == []
    print("[brain_hydrator] ok")
