from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from .io import ensure_dir


def build_markdown_report(
    run_id: str,
    profile: Dict[str, Any],
    quality_issues: List[Dict[str, Any]],
    cleaning_config: Optional[Dict[str, Any]],
    analysis_summary: Dict[str, Any],
    charts: List[Dict[str, str]],
) -> str:
    lines: List[str] = []
    lines.append(f"# Data Analysis Report")
    lines.append("")
    lines.append(f"**Run ID:** {run_id}")
    lines.append(f"**Generated:** {datetime.utcnow().isoformat()}Z")
    lines.append("")

    lines.append("## Dataset Profile")
    lines.append(f"- Rows: {profile.get('row_count')}")
    lines.append(f"- Columns: {profile.get('col_count')}")
    lines.append("")
    lines.append("### Schema")
    schema = profile.get("schema", {})
    for k, v in schema.items():
        lines.append(f"- `{k}`: `{v}`")

    lines.append("")
    lines.append("## Quality Issues")
    if not quality_issues:
        lines.append("No major issues detected.")
    else:
        for issue in quality_issues:
            lines.append(f"- {issue}")

    lines.append("")
    lines.append("## Cleaning Applied")
    if cleaning_config:
        lines.append("```json")
        import json
        lines.append(json.dumps(cleaning_config, indent=2, default=str))
        lines.append("```")
    else:
        lines.append("No cleaning config applied.")

    lines.append("")
    lines.append("## Analysis Summary")
    lines.append("```json")
    import json
    lines.append(json.dumps(analysis_summary, indent=2, default=str))
    lines.append("```")

    lines.append("")
    lines.append("## Charts")
    if not charts:
        lines.append("No charts generated.")
    else:
        for ch in charts:
            lines.append(f"- {ch.get('name')}: `{ch.get('path')}`")

    lines.append("")
    return "\n".join(lines)


def save_report_md(report_md: str, out_path: str) -> str:
    ensure_dir(os.path.dirname(out_path))
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report_md)
    return out_path