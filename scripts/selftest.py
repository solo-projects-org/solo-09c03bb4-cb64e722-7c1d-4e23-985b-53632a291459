"""端到端自测：在本地启动协调端 + 两院，覆盖关键流程。

运行：python scripts/selftest.py
不依赖 pytest，全部用标准库 + httpx；每个检查点打印 PASS/FAIL。
"""
from __future__ import annotations

import csv
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
RUNTIME = ROOT / "data" / "runtime"
SAMPLE = ROOT / "sample_data"
PYTHON = sys.executable

PORT_A = 18101
PORT_B = 18102
PORT_C = 18100

BASES = {
    "A": f"http://127.0.0.1:{PORT_A}",
    "B": f"http://127.0.0.1:{PORT_B}",
    "C": f"http://127.0.0.1:{PORT_C}",
}

results: list[tuple[str, bool, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    results.append((name, bool(cond), detail))
    flag = "PASS" if cond else "FAIL"
    print(f"[{flag}] {name}" + (f" -- {detail}" if detail else ""))


def wait_ready(url: str, timeout: float = 20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(f"{url}/health", timeout=1.0).status_code == 200:
                return True
        except Exception:
            time.sleep(0.2)
    return False


def wait_batch(batch_id: str, timeout: float = 60.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = httpx.get(f"{BASES['C']}/batches/{batch_id}", timeout=2.0)
        data = r.json()
        if data["status"] in ("SUCCEEDED", "FAILED"):
            return data
        time.sleep(0.2)
    raise TimeoutError(f"批次 {batch_id} 未在 {timeout}s 内结束")


def start_service(role: str, port: int, env_extra: dict) -> subprocess.Popen:
    env = os.environ.copy()
    env.update(env_extra)
    env["SERVICE_ROLE"] = role
    env["PORT"] = str(port)
    env["HOST"] = "127.0.0.1"
    log = open(RUNTIME / f"{role}-{port}.log", "w")
    return subprocess.Popen(
        [PYTHON, "run.py"],
        cwd=ROOT,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,
    )


def stop_service(proc: subprocess.Popen) -> None:
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=10)
    except Exception:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)


def import_csv(hospital: str, csv_path: Path) -> dict:
    with open(csv_path, "rb") as fh:
        r = httpx.post(
            f"{BASES[hospital]}/imports",
            files={"file": (csv_path.name, fh, "text/csv")},
            timeout=10.0,
        )
    assert r.status_code == 201, r.text
    return r.json()


def pooled_median(csv_files: list[Path], date_from: str, date_to: str):
    """本地独立计算预期合并中位数（与服务端算法无关的对照）。"""
    values: list[int] = []
    for path in csv_files:
        with open(path, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                if date_from <= row["date"] <= date_to:
                    values.append(int(row["value"]))
    values.sort()
    n = len(values)
    if n == 0:
        return None, 0
    if n % 2 == 1:
        med = float(values[n // 2])
    else:
        med = (values[n // 2 - 1] + values[n // 2]) / 2.0
    return med, n


def main() -> int:
    RUNTIME.mkdir(parents=True, exist_ok=True)
    for p in RUNTIME.glob("*.sqlite3*"):
        p.unlink()

    procs = []
    try:
        procs.append(start_service("hospital", PORT_A, {
            "HOSPITAL_ID": "A",
            "HOSPITAL_DB": str(RUNTIME / "a.sqlite3"),
            "SNAPSHOT_TTL_SECONDS": "3600",
        }))
        procs.append(start_service("hospital", PORT_B, {
            "HOSPITAL_ID": "B",
            "HOSPITAL_DB": str(RUNTIME / "b.sqlite3"),
            "SNAPSHOT_TTL_SECONDS": "3600",
        }))
        assert wait_ready(BASES["A"]) and wait_ready(BASES["B"]), "院端启动失败"
        procs.append(start_service("coordinator", PORT_C, {
            "COORDINATOR_DB": str(RUNTIME / "c.sqlite3"),
            "HOSPITAL_A_URL": BASES["A"],
            "HOSPITAL_B_URL": BASES["B"],
            "COORDINATOR_TIMEOUT": "3",
            "COORDINATOR_MAX_ATTEMPTS": "3",
            "COORDINATOR_POLL_INTERVAL": "0.1",
        }))
        assert wait_ready(BASES["C"]), "协调端启动失败"
        print("== 三个服务已启动 ==")

        # 1. 整版导入
        r_a = import_csv("A", SAMPLE / "hospital_A_v1.csv")
        r_b = import_csv("B", SAMPLE / "hospital_B_v1.csv")
        check("CSV 整版导入（两院 v1）", r_a["row_count"] == 120 and r_b["row_count"] == 100,
              f"A={r_a['row_count']} B={r_b['row_count']}")

        # 2. 非法 CSV 被拒（422）
        bad = RUNTIME / "bad.csv"
        bad.write_text("test_id,date,value\nX1,2026-01-01,9999\n", encoding="utf-8")
        with open(bad, "rb") as fh:
            r = httpx.post(f"{BASES['A']}/imports",
                           files={"file": ("bad.csv", fh, "text/csv")})
        check("非法 value(>1000) 返回 422", r.status_code == 422, r.text[:120])

        bad2 = RUNTIME / "bad2.csv"
        bad2.write_text(
            "test_id,date,value\n"
            "D1,2026-01-01,55\nD1,2026-01-02,60\n"
            "D2,2026/01/03,70\n",
            encoding="utf-8",
        )
        with open(bad2, "rb") as fh:
            r2 = httpx.post(f"{BASES['A']}/imports",
                            files={"file": ("bad2.csv", fh, "text/csv")})
        check("重复检验号/错误日期格式返回 422", r2.status_code == 422, r2.text[:140])

        empty = RUNTIME / "empty.csv"
        empty.write_text("test_id,date,value\n", encoding="utf-8")
        with open(empty, "rb") as fh:
            r3 = httpx.post(f"{BASES['A']}/imports",
                            files={"file": ("empty.csv", fh, "text/csv")})
        check("空数据 CSV 返回 422", r3.status_code == 422, r3.text[:120])

        # 3. 正常批次：中位数与独立计算一致
        d_from, d_to = "2026-01-01", "2026-03-31"
        r = httpx.post(f"{BASES['C']}/batches",
                       json={"date_from": d_from, "date_to": d_to}, timeout=5.0)
        b1 = r.json()
        check("发起研究返回 201/QUEUED", r.status_code == 201 and b1["status"] == "QUEUED")
        final = wait_batch(b1["batch_id"])
        expect_med, expect_n = pooled_median(
            [SAMPLE / "hospital_A_v1.csv", SAMPLE / "hospital_B_v1.csv"], d_from, d_to
        )
        check("批次成功且两院快照已固定",
              final["status"] == "SUCCEEDED"
              and {s["hospital_id"] for s in final["snapshots"]} == {"A", "B"}
              and all(s["version_tag"] == "v1" for s in final["snapshots"]),
              str(final["snapshots"]))
        check("合并样本数正确", final["pooled_sample_size"] == expect_n,
              f"{final['pooled_sample_size']} vs {expect_n}")
        check("合并中位数与独立计算一致（偶数取中间两值平均）",
              abs(final["median_value"] - expect_med) < 1e-9,
              f"{final['median_value']} vs {expect_med}")
        check("中位数 mmol/L 换算正确",
              abs(final["median_mmol_l"] - round(expect_med * 0.1, 2)) < 1e-9,
              str(final["median_mmol_l"]))

        # 4. 成功报告留存两院版号/样本数/中位数
        rep = httpx.get(f"{BASES['C']}/reports/{b1['batch_id']}").json()
        check("报告留存两院版号、样本数与中位数",
              rep["version_tag_a"] == "v1" and rep["version_tag_b"] == "v1"
              and rep["sample_size_a"] + rep["sample_size_b"] == expect_n
              and rep["median_value"] == final["median_value"], str(rep))

        # 5. 名额已回收：两院 health free
        ha, hb = httpx.get(f"{BASES['A']}/health").json(), httpx.get(f"{BASES['B']}/health").json()
        check("批次结束两院快照名额自动回收", ha["snapshot_slot"] == "free" and hb["snapshot_slot"] == "free")

        # 6. 更正版导入后，重新发起的批次使用新版号；旧批次报告不变
        r_a2 = import_csv("A", SAMPLE / "hospital_A_v2.csv")
        check("A 院更正版 v2 可提交", r_a2["version_tag"] == "v2" and r_a2["row_count"] == 130)
        r = httpx.post(f"{BASES['C']}/batches",
                       json={"date_from": d_from, "date_to": d_to}, timeout=5.0)
        b2 = r.json()
        final2 = wait_batch(b2["batch_id"])
        check("新批次固定到新版号 v2（更正可提交，批次各自固定）",
              final2["status"] == "SUCCEEDED"
              and next(s for s in final2["snapshots"] if s["hospital_id"] == "A")["version_tag"] == "v2"
              and next(s for s in final2["snapshots"] if s["hospital_id"] == "B")["version_tag"] == "v1")
        rep1_after = httpx.get(f"{BASES['C']}/reports/{b1['batch_id']}").json()
        check("旧批次报告不受更正影响（样本固定）",
              rep1_after["version_tag_a"] == "v1" and rep1_after["median_value"] == final["median_value"])

        # 7. 合并为空 => SUCCEEDED 但无中位数
        r = httpx.post(f"{BASES['C']}/batches",
                       json={"date_from": "2030-01-01", "date_to": "2030-01-31"}, timeout=5.0)
        b3 = r.json()
        final3 = wait_batch(b3["batch_id"])
        check("合并为空：批次成功但不出中位数",
              final3["status"] == "SUCCEEDED" and final3["median_value"] is None
              and final3["pooled_sample_size"] == 0)
        rep3 = httpx.get(f"{BASES['C']}/reports/{b3['batch_id']}").json()
        check("空样本报告留存版号与样本数(0)",
              rep3["pooled_sample_size"] == 0 and rep3["median_value"] is None)

        # 8. 院端名额唯一：并发两批，第二个排队等待，最终都成功
        rb_x = httpx.post(f"{BASES['C']}/batches",
                          json={"date_from": d_from, "date_to": d_to}, timeout=5.0).json()
        rb_y = httpx.post(f"{BASES['C']}/batches",
                          json={"date_from": d_from, "date_to": d_to}, timeout=5.0).json()
        fx, fy = wait_batch(rb_x["batch_id"], 40), wait_batch(rb_y["batch_id"], 40)
        check("每院仅一个快照名额：并发批次排队后均成功",
              fx["status"] == "SUCCEEDED" and fy["status"] == "SUCCEEDED",
              f"{fx['status']}/{fy['status']}")

        # 9. 任一院明确失败 => 整批失败（内嵌模拟，调度前下发，无竞态）
        rbf = httpx.post(f"{BASES['C']}/batches", json={
            "date_from": d_from, "date_to": d_to,
            "simulation": {"hospital": "B", "phase": "acquire", "fail_status": 500},
        }, timeout=5.0).json()
        ff = wait_batch(rbf["batch_id"], 30)
        check("任一院明确失败 => 整批 FAILED 且无报告",
              ff["status"] == "FAILED" and "B" in (ff["failure_reason"] or "")
              and httpx.get(f"{BASES['C']}/reports/{rbf['batch_id']}").status_code == 404,
              ff.get("failure_reason"))
        check("失败批次仍回收两院名额",
              httpx.get(f"{BASES['A']}/health").json()["snapshot_slot"] == "free"
              and httpx.get(f"{BASES['B']}/health").json()["snapshot_slot"] == "free")

        # 10. 样本过期 => 整批失败（410，规则在 count 阶段生效，快照已真实存在）
        rbe = httpx.post(f"{BASES['C']}/batches", json={
            "date_from": d_from, "date_to": d_to,
            "simulation": {"hospital": "A", "phase": "count", "expire_now": True},
        }, timeout=5.0).json()
        fe = wait_batch(rbe["batch_id"], 30)
        check("样本过期(410) => 整批 FAILED",
              fe["status"] == "FAILED" and "过期" in (fe["failure_reason"] or ""),
              fe.get("failure_reason"))
        check("过期批次同样回收两院名额",
              httpx.get(f"{BASES['A']}/health").json()["snapshot_slot"] == "free"
              and httpx.get(f"{BASES['B']}/health").json()["snapshot_slot"] == "free")

        # 11. 回执丢失：首次 acquire 超时，原批重试找回同一 snapshot_id/generation
        rbl = httpx.post(f"{BASES['C']}/batches", json={
            "date_from": d_from, "date_to": d_to,
            "simulation": {"hospital": "A", "lose_receipt_once": True},
        }, timeout=5.0).json()
        fl = wait_batch(rbl["batch_id"], 40)
        check("回执丢失后原批重试成功（幂等找回原样本）",
              fl["status"] == "SUCCEEDED", fl.get("failure_reason", ""))
        snap_a = httpx.get(f"{BASES['A']}/snapshots/{rbl['batch_id']}").json()
        check("重试复用同一快照 ID 与代次",
              snap_a["snapshot_id"] is not None and snap_a["generation"] == 1,
              str(snap_a))

        # 12. 旧批迟到回收不能释放新批名额：代次不符被拒（此时无在飞批次，名额空闲）
        httpx.post(f"{BASES['A']}/snapshots", json={
            "batch_id": "manual-hold", "date_from": d_from, "date_to": d_to})
        check("手工占用后 A 院名额为 busy",
              httpx.get(f"{BASES['A']}/health").json()["snapshot_slot"] == "busy")
        rr = httpx.post(f"{BASES['A']}/snapshots/manual-hold/release",
                        json={"batch_id": "manual-hold", "generation": 999})
        check("旧代次迟到回收被拒（STALE_GENERATION）",
              rr.status_code == 200 and rr.json()["released"] is False
              and rr.json()["reason"] == "STALE_GENERATION", rr.text)
        check("被拒后名额仍被占用（旧批迟到回收不能释放新批名额）",
              httpx.get(f"{BASES['A']}/health").json()["snapshot_slot"] == "busy")
        rr2 = httpx.post(f"{BASES['A']}/snapshots/manual-hold/release",
                         json={"batch_id": "manual-hold", "generation": 1})
        check("正确代次可正常回收", rr2.json()["released"] is True, rr2.text)
        check("回收后名额恢复 free",
              httpx.get(f"{BASES['A']}/health").json()["snapshot_slot"] == "free")

        # 13. 迟到响应不得生成报告：B 院 count 阶段持续超长延迟，协调端重试耗尽判失败；
        #     之后院端即使迟到算出响应，协调端批次已终态，轮次/报告均不得再增长
        rbl2 = httpx.post(f"{BASES['C']}/batches", json={
            "date_from": d_from, "date_to": d_to,
            "simulation": {
                "hospital": "B", "phase": "count",
                "delay_seconds": 8.0, "persistent": True,
            },
        }, timeout=5.0).json()
        fl2 = wait_batch(rbl2["batch_id"], 90)
        rounds_at_fail = httpx.get(
            f"{BASES['C']}/batches/{rbl2['batch_id']}/rounds").json()
        check("持续延迟导致批次 FAILED 且无报告（迟到响应不得生成报告）",
              fl2["status"] == "FAILED"
              and httpx.get(f"{BASES['C']}/reports/{rbl2['batch_id']}").status_code == 404,
              fl2.get("failure_reason"))
        # 等待超过单次延迟，确认没有迟到回写污染终态
        time.sleep(9.0)
        rounds_after_wait = httpx.get(
            f"{BASES['C']}/batches/{rbl2['batch_id']}/rounds").json()
        check("迟到响应不产生新轮次、批次仍 FAILED",
              len(rounds_after_wait) == len(rounds_at_fail)
              and httpx.get(f"{BASES['C']}/batches/{rbl2['batch_id']}").json()["status"] == "FAILED",
              f"rounds {len(rounds_at_fail)} -> {len(rounds_after_wait)}")
        # 清理持久规则并确认名额已回收（release 阶段不受 count 规则影响）
        httpx.delete(f"{BASES['B']}/sim/rules/{rbl2['batch_id']}")
        check("延迟失败后两院名额仍被正确回收",
              httpx.get(f"{BASES['A']}/health").json()["snapshot_slot"] == "free"
              and httpx.get(f"{BASES['B']}/health").json()["snapshot_slot"] == "free")

        # 14. 协调端只取计数：院端不存在任何明细导出接口（仅计数/样本数）
        openapi = httpx.get(f"{BASES['A']}/openapi.json").json()
        paths = set(openapi["paths"].keys())
        leaked = [p for p in paths if "observation" in p or "values" in p]
        check("院端不暴露检验明细接口", not leaked, str(sorted(paths)))

        # 15. 奇数样本中位数（收窄日期范围凑奇数池），与独立计算一致
        odd_from, odd_to = "2026-02-01", "2026-02-03"
        rbo = httpx.post(f"{BASES['C']}/batches",
                         json={"date_from": odd_from, "date_to": odd_to}, timeout=5.0).json()
        fo = wait_batch(rbo["batch_id"])
        expect_odd, n_odd = pooled_median(
            [SAMPLE / "hospital_A_v2.csv", SAMPLE / "hospital_B_v1.csv"], odd_from, odd_to
        )
        check("奇数样本中位数为整数取值（无 .5）",
              fo["status"] == "SUCCEEDED" and n_odd % 2 == 1
              and float(fo["median_value"]).is_integer()
              and abs(fo["median_value"] - expect_odd) < 1e-9,
              f"n={n_odd} median={fo['median_value']} expect={expect_odd}")

        # 16. 协调端退出/重启自动回收院端名额：
        #     用长延迟批次制造真实在飞状态，协调端停机后重启，恢复逻辑回收残留名额
        rbh = httpx.post(f"{BASES['C']}/batches", json={
            "date_from": d_from, "date_to": d_to,
            "simulation": {"hospital": "B", "phase": "count",
                           "delay_seconds": 30.0, "persistent": True},
        }, timeout=5.0).json()
        # 等到两院都已固定样本（名额被占）
        deadline = time.time() + 15
        occupied = False
        while time.time() < deadline:
            ha = httpx.get(f"{BASES['A']}/health").json()
            hb = httpx.get(f"{BASES['B']}/health").json()
            if ha["snapshot_slot"] == "busy" and hb["snapshot_slot"] == "busy":
                occupied = True
                break
            time.sleep(0.3)
        check("在飞批次已占用两院名额", occupied)
        coordinator_proc = procs[-1]
        stop_service(coordinator_proc)
        procs.remove(coordinator_proc)
        # 优雅停机：stop() 同步完成在飞批次的名额回收
        deadline = time.time() + 12
        freed_on_stop = False
        while time.time() < deadline:
            if (httpx.get(f"{BASES['A']}/health").json()["snapshot_slot"] == "free"
                    and httpx.get(f"{BASES['B']}/health").json()["snapshot_slot"] == "free"):
                freed_on_stop = True
                break
            time.sleep(0.3)
        check("协调端退出时自动回收两院名额", freed_on_stop)
        # 清理延迟规则，避免影响重启后的内部动作
        httpx.delete(f"{BASES['B']}/sim/rules/{rbh['batch_id']}")
        # 重启协调端：启动恢复把残留 RUNNING 批次置失败并保证名额已回收
        procs.append(start_service("coordinator", PORT_C, {
            "COORDINATOR_DB": str(RUNTIME / "c.sqlite3"),
            "HOSPITAL_A_URL": BASES["A"],
            "HOSPITAL_B_URL": BASES["B"],
            "COORDINATOR_TIMEOUT": "3",
            "COORDINATOR_MAX_ATTEMPTS": "3",
            "COORDINATOR_POLL_INTERVAL": "0.1",
        }))
        assert wait_ready(BASES["C"]), "协调端重启失败"
        time.sleep(1.5)
        check("重启后两院名额保持 free",
              httpx.get(f"{BASES['A']}/health").json()["snapshot_slot"] == "free"
              and httpx.get(f"{BASES['B']}/health").json()["snapshot_slot"] == "free")
        recovered = httpx.get(f"{BASES['C']}/batches/{rbh['batch_id']}").json()
        check("中断批次被置 FAILED 且原因可追溯",
              recovered["status"] == "FAILED" and "中断" in (recovered["failure_reason"] or ""),
              recovered.get("failure_reason"))
        # 重启后新批次仍可正常跑通
        rbn2 = httpx.post(f"{BASES['C']}/batches",
                          json={"date_from": d_from, "date_to": d_to}, timeout=5.0).json()
        check("协调端重启后新批次正常成功",
              wait_batch(rbn2["batch_id"])["status"] == "SUCCEEDED")

    finally:
        for proc in reversed(procs):
            stop_service(proc)

    passed = sum(1 for _, ok, _ in results if ok)
    print(f"\n==== 自测结果：{passed}/{len(results)} PASS ====")
    for name, ok, detail in results:
        if not ok:
            print(f"  FAIL: {name} {detail}")
    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
