"""Render experiment observations as a portable HTML report."""

from collections import Counter
from html import escape
from math import isfinite
from pathlib import Path
from urllib.parse import urlsplit


def _e(value):
    return escape(str(value), quote=True)


def _n(value):
    try:
        result = float(value)
        return result if isfinite(result) else 0.0
    except (TypeError, ValueError):
        return 0.0


def _ms(value):
    return "-" if value is None else f"{_n(value):,.1f} ms"


def _mib(value):
    return "-" if value is None else f"{_n(value) / 1048576:,.1f} MiB"


def _link(url, label):
    address = str(url or "")
    safe = address if urlsplit(address).scheme in ("http", "https") else "#"
    return f'<a href="{_e(safe)}">{_e(label)}</a>'


def _status(case):
    counts = Counter(str(r.get("status", "unknown")) for r in case.get("requests", []))
    return ", ".join(f"{count} {name}" for name, count in sorted(counts.items())) or "No requests"


def _timeline(requests):
    if not requests:
        return '<p class="empty">No request timings recorded.</p>'
    origin = min(_n(r.get("submitted_ms")) for r in requests)
    extent = max(1.0, max(_n(r.get("finished_ms")) for r in requests) - origin)
    left, width, height = 125, 650, 56 + 34 * len(requests)
    x = lambda value: left + max(0, min(extent, _n(value) - origin)) / extent * width
    parts = [f'<svg viewBox="0 0 900 {height}" role="img"><title>Request queue and query times</title>']
    for i in range(5):
        tick = left + i * width / 4
        parts.append(f'<line x1="{tick}" x2="{tick}" y1="27" y2="{height - 22}" class="grid"/>')
        parts.append(f'<text x="{tick}" y="18" text-anchor="middle">{extent * i / 4:,.0f} ms</text>')
    for i, request in enumerate(requests):
        y = 42 + i * 34
        submitted, started, finished = (x(request.get(key)) for key in ("submitted_ms", "started_ms", "finished_ms"))
        color = "#187d70" if request.get("status") == "ok" else "#c14a51"
        label = f"{request.get('id', '?')} / c{request.get('cursor', '?')}"
        parts.append(f'<text x="8" y="{y + 4}">{_e(label)}</text>')
        parts.append(f'<rect x="{submitted:.2f}" y="{y - 7}" width="{max(0, started - submitted):.2f}" height="14" fill="#a9b5c5"/>')
        parts.append(f'<rect x="{started:.2f}" y="{y - 7}" width="{max(1, finished - started):.2f}" height="14" fill="{color}"><title>{_e(request.get("status"))}: {_ms(request.get("query_ms"))}</title></rect>')
        parts.append(f'<text x="795" y="{y + 4}">{_e(_ms(request.get("total_ms")))}</text>')
    return "".join(parts) + "</svg>"


def _history(samples, field, title, color, profile_peak=None):
    if not samples:
        return f'<div><h4>{_e(title)}</h4><p class="empty">No samples recorded.</p></div>'
    start = min(_n(s.get("ms")) for s in samples)
    duration = max(1.0, max(_n(s.get("ms")) for s in samples) - start)
    peak = max(1.0, max(_n(s.get(field)) for s in samples), _n(profile_peak))
    points = " ".join(f"{58 + (_n(s.get('ms')) - start) / duration * 440:.2f},{155 - _n(s.get(field)) / peak * 125:.2f}" for s in sorted(samples, key=lambda s: _n(s.get("ms"))))
    parts = [f'<div><h4>{_e(title)}</h4><svg viewBox="0 0 525 190" role="img"><title>{_e(title)}</title>']
    for fraction in (0, 0.5, 1):
        y = 155 - fraction * 125
        parts.append(f'<line x1="58" x2="498" y1="{y}" y2="{y}" class="grid"/><text x="50" y="{y + 4}" text-anchor="end">{peak * fraction / 1048576:,.1f}</text>')
    if profile_peak is not None:
        y = 155 - _n(profile_peak) / peak * 125
        parts.append(f'<line x1="58" x2="498" y1="{y:.2f}" y2="{y:.2f}" stroke="#94652c" stroke-dasharray="5 4"><title>Engine profile peak: {_mib(profile_peak)}</title></line>')
    parts.append(f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="2"/>')
    parts.append(f'<text x="58" y="179">0 ms</text><text x="498" y="179" text-anchor="end">{duration:,.0f} ms</text><text x="8" y="17">MiB</text></svg></div>')
    return "".join(parts)


def write_report(report: dict, destination: Path):
    """Write a self-contained report; all free-form observations are escaped."""
    cases = report.get("cases", [])
    rows, sections = [], []
    for number, case in enumerate(cases, 1):
        survived = "Yes" if case.get("engine_survived") is True else "No" if case.get("engine_survived") is False else "Unknown"
        cells = [f'<a href="#case-{number}">{_e(case.get("label", case.get("topology")))}</a>', _e(case.get("mode")), _e(case.get("memory_limit")), _e(_status(case)), _ms(case.get("elapsed_ms")), _mib(case.get("rss_peak_bytes")), _mib(case.get("sampled_spill_peak_bytes")), _mib(case.get("profile_spill_peak_bytes")), _e(case.get("max_active_requests", "-")), survived]
        rows.append("<tr>" + "".join(f"<td>{value}</td>" for value in cells) + "</tr>")
        request_rows, errors = [], []
        for request in case.get("requests", []):
            values = [_e(request.get("id", "-")), _e(request.get("cursor", "-")), _e(request.get("status", "unknown")), _ms(request.get("queue_ms")), _ms(request.get("query_ms")), _ms(request.get("total_ms")), _e(request.get("rows") if request.get("rows") is not None else "-")]
            request_rows.append("<tr>" + "".join(f"<td>{value}</td>" for value in values) + "</tr>")
            if request.get("error"):
                errors.append(f'<details><summary>Request {_e(request.get("id"))}: {_e(request.get("status"))}</summary><pre>{_e(request["error"])}</pre></details>')
        settings = ", ".join(f"{key}={value}" for key, value in sorted(case.get("effective_settings", {}).items()))
        sections.append(f'''<section id="case-{number}">
<div class="section-heading"><h2>{_e(case.get("label", case.get("topology")))}</h2><span>{_e(case.get("mode"))} / {_e(case.get("memory_limit"))}</span></div>
<p class="outcome">Observed: {_e(_status(case))}. Engine survived: {survived}. Maximum active requests: {_e(case.get("max_active_requests", "-"))}.</p>
<p class="meta">Effective settings: {_e(settings or "Not recorded")}</p>
<h3>Request Latency</h3><p class="legend"><i class="queue"></i>Queue <i class="query"></i>Query completed <i class="failure"></i>Query failed</p>
<div class="timeline">{_timeline(case.get("requests", []))}</div>
<div class="histories">{_history(case.get("samples", []), "rss_bytes", "Process RSS (sampled)", "#187d70")}{_history(case.get("samples", []), "spill_bytes", "Spill storage (sampled)", "#4569a2", case.get("profile_spill_peak_bytes"))}</div>
<p class="meta">Engine profile buffer peak: {_mib(case.get("profile_buffer_peak_bytes"))}. Engine profile spill peak: {_mib(case.get("profile_spill_peak_bytes"))}. Dashed line: engine profile spill peak.</p>
<div class="table-wrap"><table><thead><tr><th>Request</th><th>Cursor</th><th>Status</th><th>Queue</th><th>Query</th><th>Total</th><th>Rows</th></tr></thead><tbody>{"".join(request_rows)}</tbody></table></div>
{"".join(errors) or '<p class="meta">No request errors recorded.</p>'}</section>''')
    settings = ", ".join(f"{key}={value}" for key, value in sorted(report.get("common_settings", {}).items()))
    html = f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>DuckDB Memory Experiment</title>
<style>
*{{box-sizing:border-box}}body{{margin:0;background:#fff;color:#202a34;font:14px/1.5 system-ui,sans-serif}}main{{max-width:1250px;margin:auto;padding:30px 28px 60px}}h1{{font-size:26px;margin:0 0 7px}}h2{{font-size:19px;margin:0}}h3{{font-size:15px;margin:24px 0 8px}}h4{{font-size:14px;margin:10px 0}}p{{margin:8px 0}}a{{color:#285e96}}header{{border-bottom:1px solid #dce2e8;padding-bottom:22px}}.meta,.empty{{color:#626e79;font-size:12px;overflow-wrap:anywhere}}.table-wrap{{overflow:auto;margin:16px 0}}table{{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums;font-size:12px}}th,td{{text-align:left;padding:10px 12px;border-bottom:1px solid #e3e7eb;white-space:nowrap}}th{{background:#f4f6f8;color:#465360;font-weight:600}}section{{border-top:1px solid #dce2e8;margin-top:32px;padding-top:24px;scroll-margin-top:18px}}.section-heading{{display:flex;align-items:baseline;justify-content:space-between;gap:12px}}.section-heading span{{font-size:13px;color:#626e79}}.outcome{{font-weight:600}}.legend{{display:flex;align-items:center;gap:8px;color:#626e79;font-size:12px;flex-wrap:wrap}}.legend i{{width:14px;height:10px;display:inline-block;margin-left:10px}}.legend i:first-child{{margin-left:0}}.queue{{background:#a9b5c5}}.query{{background:#187d70}}.failure{{background:#c14a51}}svg{{display:block;width:100%;height:auto}}svg text{{font:11px system-ui,sans-serif;fill:#626e79}}.grid{{stroke:#e5e9ed;stroke-width:1}}.timeline{{overflow-x:auto}}.timeline svg{{min-width:620px}}.histories{{display:grid;grid-template-columns:1fr 1fr;gap:28px}}details{{border:1px solid #e0e5ea;border-radius:4px;margin:10px 0;padding:10px 12px}}summary{{cursor:pointer;font-weight:600;font-size:12px}}pre{{background:#f5f7f9;border:1px solid #e4e8ed;padding:14px;overflow:auto;white-space:pre-wrap;overflow-wrap:anywhere;font:12px/1.6 ui-monospace,monospace}}footer{{border-top:1px solid #dce2e8;margin-top:32px;padding-top:18px;color:#626e79;font-size:12px}}@media(max-width:700px){{main{{padding:20px 14px 40px}}.histories{{grid-template-columns:1fr;gap:8px}}.section-heading{{align-items:flex-start;flex-direction:column;gap:4px}}}}
</style></head><body><main><header><h1>DuckDB Memory Experiment</h1>
<p class="meta">Created {_e(report.get("created_at", "-"))} | DuckDB {_e(report.get("duckdb_version", "-"))} | Python {_e(report.get("python_version", "-"))} | {_e(report.get("platform", "-"))}</p>
<p>{_link(report.get("source_page"), "NYC TLC source page")} / {_link(report.get("source_url"), "Dataset download")}</p>
<p class="meta">Dataset: {_e(report.get("row_count", "-"))} rows, {_e(report.get("data_bytes", "-"))} bytes. SHA-256: {_e(report.get("data_sha256", "-"))}</p>
<p class="meta">Common settings: {_e(settings or "Not recorded")}</p></header>
<p class="meta">{_e(report.get('common_settings', {}).get('requests', '-'))} simultaneous Python requests, round-robin cursor assignment, one whole-request lock per cursor. All connections use the same database file and share one engine budget. Each topology/scenario runs in a fresh process. Query timing includes profiling setup and Parquet export; dataset download and connection setup are excluded. RSS is sampled every 50 ms. This is a small observation, not a statistical benchmark.</p>
<h3>Observed Outcomes</h3><div class="table-wrap"><table><thead><tr><th>Topology</th><th>Budget scenario</th><th>Memory limit</th><th>Observed statuses</th><th>Elapsed</th><th>RSS sampled peak</th><th>Spill sampled peak</th><th>Spill profile peak</th><th>Max active</th><th>Survived</th></tr></thead><tbody>{"".join(rows)}</tbody></table></div>
<p class="meta">Scenario names describe the intended budget test. Statuses and peaks describe the actual observations. RSS includes process memory outside DuckDB's buffer manager. Sampled spill can miss short peaks; profile values report engine peaks and may include overlapping requests in a shared instance.</p>
<details><summary>Workload SQL</summary><pre>{_e(report.get("query", "Not recorded"))}</pre></details>
{"".join(sections)}<footer>References: {_link("https://duckdb.org/docs/current/guides/performance/how_to_tune_workloads", "DuckDB workload tuning")}, {_link("https://duckdb.org/docs/current/guides/troubleshooting/oom_errors", "DuckDB OOM errors")}, {_link("https://duckdb.org/docs/current/dev/metrics", "DuckDB profiling metrics")}. Measurements apply to this dataset, query, version and environment.</footer>
</main></body></html>'''
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(html, encoding="utf-8")
