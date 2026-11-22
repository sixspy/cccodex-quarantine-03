from __future__ import annotations

import sqlite3
import threading

import pytest

from app.db import get_db
from app.services import captures, cleaning, quarantine, rework, review, settings
from app.services.errors import ServiceError


def _enqueue_rework(db_path: str):
    """造一条待复核隔离行并开回炉单，返回 (row, job)。"""
    batches = captures.list_batches(db_path)
    batch = next(b for b in batches if b["status"] != "已关闭")
    open_bin = next(b for b in quarantine.list_bins(db_path) if b["status"] == "开放")
    detail = captures.get_batch(batch["id"], db_path)
    bad = next(i for i in detail["items"] if i["quality"] != "合格" and i["status"] == "待检")
    row = quarantine.enter_item(bad["id"], open_bin["id"], "缺字段-测试", db_path)
    job = rework.enqueue_from_row(row["id"], db_path)
    return row, job, bad


def test_run_pins_materialized_snapshot(db_path):
    row, job, item = _enqueue_rework(db_path)
    result = rework.run_job(job["id"], succeed=True, db_path=db_path)
    assert result["status"] == "成功"

    jobs = rework.list_jobs(db_path)
    pinned = next(j for j in jobs if j["id"] == job["id"])
    # 列表读数来自物化包，不是活态隔离行
    assert pinned["pin_source"] == "物化包"
    assert pinned["pin_reason_code"] == "缺字段-测试"
    assert pinned["pin_fingerprint"].startswith("sha256:")
    assert pinned["pinned_at"]

    with get_db(db_path) as conn:
        pin = conn.execute("SELECT * FROM rework_pins WHERE job_id=?", (job["id"],)).fetchone()
        assert pin["item_key"] == item["item_key"]
        # 回炉成功后未直接放行，留待清洗入库闸口
        qrow = conn.execute("SELECT * FROM quarantine_rows WHERE id=?", (row["id"],)).fetchone()
        assert qrow["status"] == "回炉中"


def test_pin_is_immutable_reject_second_run(db_path):
    _, job, _ = _enqueue_rework(db_path)
    rework.run_job(job["id"], succeed=False, db_path=db_path)
    first = next(j for j in rework.list_jobs(db_path) if j["id"] == job["id"])
    assert first["pin_source"] == "物化包"
    pinned_at = first["pinned_at"]
    fingerprint = first["pin_fingerprint"]

    # 失败后重试：复用同一条不可改钉点，不另开第二条
    retried = rework.run_job(job["id"], succeed=True, db_path=db_path)
    assert retried["status"] == "成功"
    second = next(j for j in rework.list_jobs(db_path) if j["id"] == job["id"])
    assert second["pinned_at"] == pinned_at
    assert second["pin_fingerprint"] == fingerprint

    with get_db(db_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) AS c FROM rework_pins WHERE job_id=?", (job["id"],)
        ).fetchone()["c"] == 1

    # 成功后再跑被状态闸口挡回
    with pytest.raises(ServiceError) as exc:
        rework.run_job(job["id"], succeed=True, db_path=db_path)
    assert exc.value.status_code == 409


def test_fingerprint_mismatch_blocks_cleaning_intake(db_path):
    row, job, item = _enqueue_rework(db_path)
    rework.run_job(job["id"], succeed=True, db_path=db_path)

    # 抓取侧漂件：开跑后载荷被换掉，指纹拧花
    with get_db(db_path) as conn:
        conn.execute(
            "UPDATE capture_items SET payload=? WHERE id=?",
            ('{"title":"被漂走的载荷"}', item["id"]),
        )

    with pytest.raises(ServiceError) as exc:
        cleaning.intake_item(item["id"], db_path)
    assert exc.value.status_code == 409
    assert "指纹" in exc.value.message

    with get_db(db_path) as conn:
        cur = conn.execute(
            "SELECT * FROM capture_items WHERE id=?", (item["id"],)
        ).fetchone()
        assert cur["status"] == "回炉中"


def test_fingerprint_match_allows_cleaning_intake(db_path):
    row, job, item = _enqueue_rework(db_path)
    rework.run_job(job["id"], succeed=True, db_path=db_path)
    out = cleaning.intake_item(item["id"], db_path)
    assert out["status"] == "已放行"
    with get_db(db_path) as conn:
        qrow = conn.execute(
            "SELECT * FROM quarantine_rows WHERE id=?", (row["id"],)
        ).fetchone()
        assert qrow["status"] == "已放行"


def test_concurrent_enqueue_only_one_active_job(db_path):
    row, _, _ = _enqueue_rework(db_path)
    # 已有活跃回炉单时，并发再开单只能成一张
    errors = []

    def worker():
        try:
            rework.enqueue_from_row(row["id"], db_path)
        except ServiceError as exc:
            errors.append(exc.message)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    with get_db(db_path) as conn:
        active = conn.execute(
            "SELECT COUNT(*) AS c FROM rework_jobs WHERE row_id=? AND status IN ('排队','执行中')",
            (row["id"],),
        ).fetchone()["c"]
    assert active == 1
    assert errors, "并发开单必须有一方被排他锁挡回"


def test_concurrent_run_double_pin_prevented(db_path):
    _, job, _ = _enqueue_rework(db_path)
    outcomes = []

    def worker():
        try:
            rework.run_job(job["id"], succeed=True, db_path=db_path)
            outcomes.append("ok")
        except ServiceError as exc:
            outcomes.append(exc.message)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert outcomes.count("ok") == 1
    with get_db(db_path) as conn:
        pin_count = conn.execute(
            "SELECT COUNT(*) AS c FROM rework_pins WHERE job_id=?", (job["id"],)
        ).fetchone()["c"]
        job_row = conn.execute(
            "SELECT * FROM rework_jobs WHERE id=?", (job["id"],)
        ).fetchone()
    assert pin_count == 1
    assert job_row["status"] == "成功"


def test_list_reads_from_pin_when_live_row_drifts(db_path):
    row, job, item = _enqueue_rework(db_path)
    rework.run_job(job["id"], succeed=True, db_path=db_path)
    # 活态隔离行的原因码与条目被改动，列表读数仍须来自物化包
    with get_db(db_path) as conn:
        conn.execute(
            "UPDATE quarantine_rows SET reason=? WHERE id=?",
            ("活态原因被漂改", row["id"]),
        )
        conn.execute(
            "UPDATE capture_items SET item_key=? WHERE id=?",
            ("sku-drifted", item["id"]),
        )
    mine = next(j for j in rework.list_jobs(db_path) if j["id"] == job["id"])
    assert mine["item_key"] == item["item_key"]
    assert mine["reason"] == "缺字段-测试"
    assert mine["pin_source"] == "物化包"


def test_pin_mode_setting_readonly(db_path):
    items = {s["key"]: s for s in settings.list_settings(db_path)}
    assert items["rework_pin_mode"]["readonly"] == 1
    assert items["rework_pin_mode"]["value"] == "物化包"
    with pytest.raises(ServiceError) as exc:
        settings.update_setting("rework_pin_mode", "独立凭证", db_path)
    assert exc.value.status_code == 403
    assert settings.get_value("rework_pin_mode", db_path=db_path) == "物化包"


def test_bin_reworking_count(db_path):
    row, job, _ = _enqueue_rework(db_path)
    bins = {b["id"]: b for b in quarantine.list_bins(db_path)}
    assert bins[row["bin_id"]]["reworking_count"] >= 1


def test_review_reject_respects_active_lock(db_path):
    row, _, _ = _enqueue_rework(db_path)
    # 模拟状态被拨回待复核但活跃回炉单仍在：唯一索引必须挡住重复开单
    with get_db(db_path) as conn:
        conn.execute("UPDATE quarantine_rows SET status='待复核' WHERE id=?", (row["id"],))
    tickets = review.list_tickets(db_path)
    ticket = next(t for t in tickets if t["row_id"] == row["id"] and t["decision"] == "待决")
    with pytest.raises(ServiceError) as exc:
        review.decide(ticket["id"], "驳回回炉", "tester", "再开一单", db_path)
    assert exc.value.status_code == 409
    with get_db(db_path) as conn:
        active = conn.execute(
            "SELECT COUNT(*) AS c FROM rework_jobs WHERE row_id=? AND status IN ('排队','执行中')",
            (row["id"],),
        ).fetchone()["c"]
    assert active == 1


def test_old_db_migrates_pin_table_and_setting(tmp_path):
    # 模拟老库：无 rework_pins 表、无只读参数，ensure_db 后应补齐
    old = tmp_path / "old.db"
    conn = sqlite3.connect(old)
    conn.executescript(
        """
        CREATE TABLE lab_settings(key TEXT PRIMARY KEY, value TEXT NOT NULL, label TEXT NOT NULL);
        CREATE TABLE rework_jobs(
          id INTEGER PRIMARY KEY AUTOINCREMENT, job_code TEXT, row_id INTEGER,
          status TEXT, attempt INTEGER DEFAULT 1, message TEXT DEFAULT '',
          started_at TEXT, finished_at TEXT, created_at TEXT
        );
        """
    )
    conn.commit()
    conn.close()

    from app import config

    config.Config.DATABASE_PATH = old
    config.Config.INSTANCE_DIR = tmp_path
    from app.db import ensure_db

    ensure_db()
    with get_db(str(old)) as c:
        names = {r[0] for r in c.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        assert "rework_pins" in names
        mode = c.execute(
            "SELECT value FROM lab_settings WHERE key='rework_pin_mode'"
        ).fetchone()
        assert mode["value"] == "物化包"
