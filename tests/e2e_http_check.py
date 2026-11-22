from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

WORKSPACE = Path("/workspace")
DB_DIR = WORKSPACE / "instance_e2e"
DB_DIR.mkdir(exist_ok=True)
DB_PATH = DB_DIR / "e2e.db"
if DB_PATH.exists():
    DB_PATH.unlink()

PORT = 7519


def wait_port(port: int, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(0.3)
    raise RuntimeError("服务未在预期时间内启动")


env = dict(os.environ)
env.update(
    {
        "DATABASE_PATH": str(DB_PATH),
        "INSTANCE_DIR": str(DB_DIR),
        "PORT": str(PORT),
        "HOST": "127.0.0.1",
    }
)
proc = subprocess.Popen(
    [sys.executable, "run.py"],
    cwd=WORKSPACE,
    env=env,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
)
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name} {detail}")
    if not cond:
        failures.append(name)


try:
    wait_port(PORT)
    base = f"http://127.0.0.1:{PORT}"
    with httpx.Client(base_url=base, timeout=10) as c:
        # 未登录被拦
        r = c.get("/api/rework/jobs")
        check("未登录拦截", r.status_code == 401, str(r.status_code))

        r = c.post("/api/login", json={"username": "admin", "password": "123456"})
        check("登录成功", r.status_code == 200 and r.json()["ok"])

        # 取一个待检坏件入舱
        batches = c.get("/api/batches").json()["items"]
        batch = next(b for b in batches if b["status"] != "已关闭")
        detail = c.get(f"/api/batches/{batch['id']}").json()
        bad = next(i for i in detail["items"] if i["quality"] != "合格" and i["status"] == "待检")
        check("抓取详情带样本指纹", bad["payload_fingerprint"].startswith("sha256:"), bad["item_key"])

        bins = c.get("/api/bins").json()["items"]
        open_bin = next(b for b in bins if b["status"] == "开放")
        r = c.post("/api/quarantine/enter", json={"item_id": bad["id"], "bin_id": open_bin["id"], "reason": "E2E缺字段"})
        check("入舱成功", r.status_code == 200)
        row = r.json()

        # 入回炉队列
        r = c.post("/api/rework/enqueue", json={"row_id": row["id"]})
        check("入回炉队列", r.status_code == 200)
        job = r.json()

        # 并发抢开同一回炉单
        def call_run():
            return c.post(f"/api/rework/jobs/{job['id']}/run", json={"succeed": True})

        import threading

        results = []

        def worker():
            results.append(call_run())

        ts = [threading.Thread(target=worker) for _ in range(5)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        ok = [x for x in results if x.status_code == 200]
        conflict = [x for x in results if x.status_code == 409]
        check("并发开跑只成一单", len(ok) == 1 and len(conflict) == 4,
              f"ok={len(ok)} conflict={len(conflict)}")
        if conflict:
            check("冲突返回短句中文", "开跑" in conflict[0].json()["error"], conflict[0].json()["error"])

        # 回炉列表读数换源：钉点徽标
        jobs = c.get("/api/rework/jobs").json()["items"]
        mine = next(j for j in jobs if j["id"] == job["id"])
        check("钉点出处徽标=物化包", mine["pin_source"] == "物化包")
        check("钉点封存原因码", mine["pin_reason_code"] == "E2E缺字段", mine.get("pin_reason_code", ""))
        check("钉点封存条目键", mine["item_key"] == bad["item_key"])

        # 隔离舱卡片回炉中计数
        bins2 = {b["id"]: b for b in c.get("/api/bins").json()["items"]}
        check("隔离舱回炉中计数", bins2[open_bin["id"]]["reworking_count"] >= 1,
              str(bins2[open_bin["id"]]["reworking_count"]))

        # 库内须有物化包行
        import sqlite3

        con = sqlite3.connect(DB_PATH)
        pin_cnt = con.execute("SELECT COUNT(*) FROM rework_pins WHERE job_id=?", (job["id"],)).fetchone()[0]
        active_cnt = con.execute(
            "SELECT COUNT(*) FROM rework_jobs WHERE row_id=?",
            (row["id"],),
        ).fetchone()[0]
        con.close()
        check("库内物化包行=1", pin_cnt == 1, str(pin_cnt))
        check("并发未双写回炉单", active_cnt == 1, str(active_cnt))

        # 漂件：换掉载荷 → 指纹拧花 → 清洗入库被拒
        con = sqlite3.connect(DB_PATH)
        con.execute("UPDATE capture_items SET payload=? WHERE id=?", ('{"title":"漂件"}', bad["id"]))
        con.commit()
        con.close()
        r = c.post("/api/cleaning/intake", json={"item_id": bad["id"]})
        check("指纹拧花拒清洗入库", r.status_code == 409 and "指纹" in r.json()["error"], r.json().get("error", ""))

        # 还原载荷 → 指纹与钉点一致 → 入库放行
        con = sqlite3.connect(DB_PATH)
        con.execute("UPDATE capture_items SET payload=? WHERE id=?", (bad["payload"], bad["id"]))
        con.commit()
        con.close()
        r = c.post("/api/cleaning/intake", json={"item_id": bad["id"]})
        check("指纹一致清洗入库放行", r.status_code == 200 and r.json()["status"] == "已放行")

        # 设置里钉点口径只读
        slist = c.get("/api/settings").json()["items"]
        mode = next(s for s in slist if s["key"] == "rework_pin_mode")
        check("钉点口径只读标记", mode["readonly"] == 1 and mode["value"] == "物化包")
        r = c.post("/api/settings/update", json={"key": "rework_pin_mode", "value": "独立凭证"})
        check("服务端拒改只读口径", r.status_code == 403 and "只读" in r.json()["error"], r.json().get("error", ""))

        # 审计页含钉点封存与入库被拒事件
        audits = c.get("/api/audits?limit=200").json()["items"]
        actions = {a["action"] for a in audits}
        check("审计含回炉钉点封存", "回炉钉点封存" in actions, str(sorted(actions)))
        check("审计含清洗入库被拒", "清洗入库被拒" in actions)

        # 再抽第二单走完整链路，比对左右读数
        detail2 = c.get(f"/api/batches/{batch['id']}").json()
        bad2 = next((i for i in detail2["items"] if i["quality"] != "合格" and i["status"] == "待检"), None)
        if bad2:
            r = c.post("/api/quarantine/enter", json={"item_id": bad2["id"], "bin_id": open_bin["id"], "reason": "E2E第二单"})
            row2 = r.json()
            job2 = c.post("/api/rework/enqueue", json={"row_id": row2["id"]}).json()
            r = c.post(f"/api/rework/jobs/{job2['id']}/run", json={"succeed": True})
            check("第二单开跑成功", r.status_code == 200)
            r = c.post("/api/cleaning/intake", json={"item_id": bad2["id"]})
            check("第二单入库比对一致", r.status_code == 200 and r.json()["status"] == "已放行")
            j2 = next(j for j in c.get("/api/rework/jobs").json()["items"] if j["id"] == job2["id"])
            check("第二单钉点徽标在列", j2["pin_source"] == "物化包" and j2["pin_reason_code"] == "E2E第二单")

finally:
    proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()

print(json.dumps({"failures": failures}, ensure_ascii=False))
sys.exit(1 if failures else 0)
