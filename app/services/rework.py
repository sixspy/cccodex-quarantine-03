from __future__ import annotations

import hashlib
import sqlite3

from app.db import get_db
from app.services import audit as audit_svc
from app.services import settings as settings_svc
from app.services.errors import ServiceError

PIN_SOURCE = "物化包"


def payload_fingerprint(payload: str) -> str:
    """样本指纹：对抓取载荷原文做 sha256，钉点与入闸共用同一口径。"""
    return "sha256:" + hashlib.sha256((payload or "").encode("utf-8")).hexdigest()


def list_jobs(db_path: str | None = None) -> list[dict]:
    with get_db(db_path) as conn:
        rows = conn.execute(
            """
            SELECT j.*,
                   COALESCE(p.item_key, i.item_key) AS item_key,
                   COALESCE(p.reason_code, r.reason) AS reason,
                   b.bin_code,
                   p.pin_source, p.reason_code AS pin_reason_code,
                   p.payload_fingerprint AS pin_fingerprint, p.pinned_at
            FROM rework_jobs j
            JOIN quarantine_rows r ON r.id=j.row_id
            JOIN capture_items i ON i.id=r.item_id
            JOIN quarantine_bins b ON b.id=r.bin_id
            LEFT JOIN rework_pins p ON p.job_id=j.id
            ORDER BY j.id DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]


def _is_locked(exc: sqlite3.Error) -> bool:
    msg = str(exc).lower()
    return "locked" in msg or "busy" in msg


def run_job(job_id: int, succeed: bool = True, db_path: str | None = None) -> dict:
    """开跑回炉：服务端一次落定状态与钉点，拒绝并发双开。"""
    pin_created = False
    with get_db(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            job = conn.execute(
                "SELECT * FROM rework_jobs WHERE id=?", (job_id,)
            ).fetchone()
            if not job:
                raise ServiceError("回炉任务不存在", 404)
            if job["status"] not in ("排队", "失败"):
                raise ServiceError("任务已开跑，请勿重复执行", 409)
            max_attempt = int(
                settings_svc.get_value("rework_max_attempt", "3", db_path) or "3"
            )
            if job["attempt"] > max_attempt:
                raise ServiceError("已超过回炉最大次数")

            row = conn.execute(
                "SELECT * FROM quarantine_rows WHERE id=?", (job["row_id"],)
            ).fetchone()
            if not row:
                raise ServiceError("隔离行不存在", 404)
            item = conn.execute(
                "SELECT * FROM capture_items WHERE id=?", (row["item_id"],)
            ).fetchone()
            if not item:
                raise ServiceError("抓取条目不存在", 404)

            # 开跑即物化钉点；钉点不可改，失败重试只复用原钉点，不另开第二条
            existing_pin = conn.execute(
                "SELECT * FROM rework_pins WHERE job_id=?", (job_id,)
            ).fetchone()
            if existing_pin:
                if existing_pin["item_key"] != item["item_key"]:
                    raise ServiceError("条目键与回炉钉点不一致，禁止开跑", 409)
                fingerprint = existing_pin["payload_fingerprint"]
            else:
                fingerprint = payload_fingerprint(item["payload"])
                conn.execute(
                    """
                    INSERT INTO rework_pins(
                      job_id, item_key, reason_code, payload_fingerprint, pin_source
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (job_id, item["item_key"], row["reason"], fingerprint, PIN_SOURCE),
                )
                pin_created = True

            conn.execute(
                """
                UPDATE rework_jobs SET status='执行中', started_at=datetime('now')
                WHERE id=?
                """,
                (job_id,),
            )
            if succeed:
                # 回炉重抓完成，样本留待清洗入库闸口验指纹后才放行
                conn.execute(
                    """
                    UPDATE rework_jobs
                    SET status='成功', finished_at=datetime('now'),
                        message='回炉抓取成功，待清洗入库'
                    WHERE id=?
                    """,
                    (job_id,),
                )
            else:
                attempt = job["attempt"] + 1
                conn.execute(
                    """
                    UPDATE rework_jobs
                    SET status='失败', attempt=?, finished_at=datetime('now'),
                        message='回炉抓取失败'
                    WHERE id=?
                    """,
                    (attempt, job_id),
                )
            out = conn.execute(
                "SELECT * FROM rework_jobs WHERE id=?", (job_id,)
            ).fetchone()
            result = dict(out)
            audit_detail = f"{item['item_key']}｜{row['reason']}｜{fingerprint}"
        except ServiceError:
            raise
        except sqlite3.IntegrityError as exc:
            if _is_locked(exc):
                raise ServiceError("回炉单已被另一值班开跑，请刷新后重试", 409)
            raise ServiceError("开跑失败：数据约束冲突", 409)
        except sqlite3.Error as exc:
            if _is_locked(exc):
                raise ServiceError("回炉单已被另一值班开跑，请刷新后重试", 409)
            raise
    audit_svc.write_event(
        "执行回炉",
        "rework_job",
        job_id,
        f"{'成功' if succeed else '失败'}｜钉点{PIN_SOURCE}",
        db_path=db_path,
    )
    if pin_created:
        audit_svc.write_event(
            "回炉钉点封存", "rework_pin", job_id, audit_detail, db_path=db_path
        )
    return result


def enqueue_from_row(row_id: int, db_path: str | None = None) -> dict:
    with get_db(db_path) as conn:
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM quarantine_rows WHERE id=?", (row_id,)
            ).fetchone()
            if not row:
                raise ServiceError("隔离行不存在", 404)
            if row["status"] not in ("待复核", "回炉中"):
                raise ServiceError("当前状态不可回炉")
            active = conn.execute(
                """
                SELECT id FROM rework_jobs
                WHERE row_id=? AND status IN ('排队','执行中')
                """,
                (row_id,),
            ).fetchone()
            if active:
                raise ServiceError("已有进行中的回炉任务", 409)
            n = conn.execute("SELECT COUNT(*) AS c FROM rework_jobs").fetchone()["c"] + 1
            cur = conn.execute(
                """
                INSERT INTO rework_jobs(job_code, row_id, status, message)
                VALUES (?, ?, '排队', '手工入回炉队列')
                """,
                (f"RWK-{n:04d}", row_id),
            )
            conn.execute(
                "UPDATE quarantine_rows SET status='回炉中' WHERE id=?", (row_id,)
            )
            conn.execute(
                "UPDATE capture_items SET status='回炉中' WHERE id=?", (row["item_id"],)
            )
            out = conn.execute(
                "SELECT * FROM rework_jobs WHERE id=?", (cur.lastrowid,)
            ).fetchone()
            result = dict(out)
        except ServiceError:
            raise
        except sqlite3.IntegrityError as exc:
            if _is_locked(exc):
                raise ServiceError("回炉队列繁忙，请稍后再试", 409)
            # ux_rework_active_row 命中：另一值班已抢到同一隔离行
            raise ServiceError("该隔离行已有活跃回炉单，禁止重复开单", 409)
        except sqlite3.Error as exc:
            if _is_locked(exc):
                raise ServiceError("回炉队列繁忙，请稍后再试", 409)
            raise
    return result
