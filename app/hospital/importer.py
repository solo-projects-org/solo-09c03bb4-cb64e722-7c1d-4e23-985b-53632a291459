"""CSV 整版解析。

要求列：test_id,date,value（表头固定，顺序不限）。
- test_id：非空，版内唯一
- date：YYYY-MM-DD
- value：1..1000 的整数（0.1 mmol/L 单位）
"""
from __future__ import annotations

import csv
import io
from datetime import datetime

REQUIRED_COLUMNS = {"test_id", "date", "value"}


class CsvImportError(ValueError):
    """带行号的导入错误，HTTP 422 返回。"""


def parse_csv(content: bytes) -> list[tuple[str, str, int]]:
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvImportError(f"文件不是 UTF-8 编码: {exc}") from exc

    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise CsvImportError("CSV 缺少表头")
    headers = {h.strip().lower() for h in reader.fieldnames}
    missing = REQUIRED_COLUMNS - headers
    if missing:
        raise CsvImportError(f"CSV 缺少必需列: {sorted(missing)}")

    rows: list[tuple[str, str, int]] = []
    seen: set[str] = set()
    for line_no, raw in enumerate(reader, start=2):
        test_id = (raw.get("test_id") or "").strip()
        obs_date = (raw.get("date") or "").strip()
        value_raw = (raw.get("value") or "").strip()
        if not test_id:
            raise CsvImportError(f"第 {line_no} 行 test_id 为空")
        if test_id in seen:
            raise CsvImportError(f"第 {line_no} 行检验号重复: {test_id}")
        try:
            datetime.strptime(obs_date, "%Y-%m-%d")
        except ValueError as exc:
            raise CsvImportError(
                f"第 {line_no} 行日期格式错误（应为 YYYY-MM-DD）: {obs_date}"
            ) from exc
        try:
            value = int(value_raw)
        except ValueError as exc:
            raise CsvImportError(
                f"第 {line_no} 行 value 不是整数: {value_raw!r}"
            ) from exc
        if not 1 <= value <= 1000:
            raise CsvImportError(
                f"第 {line_no} 行 value={value} 超出 1..1000 范围"
            )
        seen.add(test_id)
        rows.append((test_id, obs_date, value))

    if not rows:
        raise CsvImportError("CSV 没有任何数据行")
    return rows
