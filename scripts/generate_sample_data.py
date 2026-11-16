"""生成两院示例 CSV（整版导入用）。

用法：python scripts/generate_sample_data.py [输出目录]
单位 0.1 mmol/L 的 1..1000 整数；检验号全院唯一。
"""
from __future__ import annotations

import csv
import os
import random
import sys
from datetime import date, timedelta

# 固定随机种子，保证可复现（自测可手工核对中位数）
SEED = 20261116

# (医院, 版本, 行数, 取值下界, 取上界, 起始日期)
SPECS = [
    ("A", "v1", 120, 40, 90, date(2026, 1, 1)),
    ("B", "v1", 100, 60, 120, date(2026, 1, 5)),
    # 更正版：A 院重新整版导入（在研批次仍固定 v1，新批次才用 v2）
    ("A", "v2", 130, 42, 95, date(2026, 1, 1)),
]


def generate(out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    rng = random.Random(SEED)
    for hospital, tag, n, lo, hi, start in SPECS:
        path = os.path.join(out_dir, f"hospital_{hospital}_{tag}.csv")
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["test_id", "date", "value"])
            for i in range(n):
                test_id = f"{hospital}-{tag}-{i + 1:05d}"
                obs_date = (start + timedelta(days=i % 80)).isoformat()
                value = rng.randint(lo, hi)
                writer.writerow([test_id, obs_date, value])
        print(f"wrote {path} ({n} rows)")


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "sample_data"
    generate(target)
