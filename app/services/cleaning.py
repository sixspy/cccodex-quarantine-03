from __future__ import annotations

from app.db import get_db
from app.services import audit as audit_svc
from app.services import rework as rework_svc
from app.services.errors import ServiceError


def intake_item(item_id: int, db_path: str | None = None) -> dict:
    """清洗入库闸口：回炉样本须与开跑钉点指纹一致，拧花即拒。"""
    with get_db(db_path) as conn:
        item = conn.execute(
            "SELECT * FROM capture_items WHERE id=?", (item_id,)
        ).fetchone()
        if not item:
            raise ServiceError("抓取条目不存在", 404)

        pin_row = conn.execute(
            """
            SELECT p.*, r.id AS quarantine_row_id FROM rework_pins p
            JOIN rework_jobs j ON j.id=p.job_id
            JOIN quarantine_rows r ON r.id=j.row_id
            WHERE r.item_id=?
            ORDER BY p.id DESC LIMIT 1
            """,
            (item_id,),
        ).fetchone()

        current_fingerprint = rework_svc.payload_fingerprint(item["payload"])
        if pin_row:
            if pin_row["payload_fingerprint"] != current_fingerprint:
                audit_svc.write_event(
                    "清洗入库被拒",
                    "capture_item",
                    item_id,
                    f"钉点指纹{pin_row['payload_fingerprint']}｜当前指纹{current_fingerprint}",
                    db_path=db_path,
                )
                raise ServiceError("样本指纹与回炉钉点不一致，拒绝清洗入库", 409)
        elif item["quality"] != "合格":
            raise ServiceError("非合格样本不可清洗入库")

        conn.execute(
            "UPDATE capture_items SET status='已放行', quality='合格' WHERE id=?",
            (item_id,),
        )
        if pin_row:
            conn.execute(
                "UPDATE quarantine_rows SET status='已放行' WHERE id=?",
                (pin_row["quarantine_row_id"],),
            )
        out = conn.execute(
            "SELECT id, item_key, quality, status, payload FROM capture_items WHERE id=?",
            (item_id,),
        ).fetchone()
        result = dict(out)
        result["payload_fingerprint"] = current_fingerprint
    audit_svc.write_event(
        "清洗入库",
        "capture_item",
        item_id,
        f"指纹{current_fingerprint}",
        db_path=db_path,
    )
    return result
