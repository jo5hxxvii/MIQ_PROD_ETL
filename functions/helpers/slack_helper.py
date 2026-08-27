import json
import urllib.request
from datetime import date, datetime

from functions.config import _config

SLACK_WEBHOOK = _config.SLACK_WEBHOOK


def send_slack(log, message: str, is_error: bool = False):
    """
    Post a message to Slack via webhook.
    Silently skips if SLACK_WEBHOOK is not set.
    Never raises — Slack failure should not affect ETL.
    """
    if not SLACK_WEBHOOK:
        return

    icon = ":x:" if is_error else ":white_check_mark:"
    payload = json.dumps({"text": f"{icon} *MIQ ETL* — {message}"}).encode()

    try:
        req = urllib.request.Request(
            SLACK_WEBHOOK,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                log.warning(f"Slack returned HTTP {resp.status}")
    except Exception as e:
        log.warning(f"Slack notification failed (non-fatal): {e}")


def slack_success(
    run_id,
    elapsed,
    rows_loaded,
    miq_summary,
    snapshot_status,
    snapshot_note,
    phases_complete,
    log_filename,
    log,
):
    total_rows = sum(rows_loaded.values())
    tables = len(rows_loaded)

    # Tables loaded
    table_lines = [f"  {tname}: {n:,}" for tname, n in rows_loaded.items()]
    tables_block = "\n".join(table_lines) if table_lines else "  (none)"

    # MIQ component results — all functions, all tables
    miq_lines = []
    for fn, result in miq_summary.items():
        for tname, n in result.items():
            val = f"{n:,}" if isinstance(n, int) else str(n)
            miq_lines.append(f"  {tname}: {val}")
    # miq_lines = []
    # for fn, result in miq_summary.items():
    #     for tname, n in result.items():
    #         miq_lines.append(f"  {tname}: {n:,}")
    miq_block = "\n".join(miq_lines) if miq_lines else "  (no MIQ data)"

    # Phases
    phases_block = ", ".join(phases_complete) if phases_complete else "none"

    # Snapshot
    snap_line = snapshot_status or "not reached"
    if snapshot_note:
        snap_line += f" — {snapshot_note}"

    msg = (
        f"Run `{run_id}` completed successfully in {elapsed / 60:.1f} minutes\n"
        f"\n"
        f"*Phases complete:* {phases_block}\n"
        f"\n"
        f"*Tables loaded:* {tables} ({total_rows:,} total rows)\n"
        f"```\n{tables_block}\n```\n"
        f"*MIQ component results:*\n"
        f"```\n{miq_block}\n```\n"
        f"*Snapshot:* {snap_line}\n"
        f"*Log file:* `{log_filename}`"
    )
    send_slack(log, msg, is_error=False)


def slack_failure(
    run_id,
    elapsed,
    phases_complete,
    failed_downloads,
    failed_tables,
    error_message,
    log_filename,
    log,
):
    """Send a failure summary to Slack."""
    phases = ", ".join(phases_complete) if phases_complete else "none"
    lines = [
        f"Run `{run_id}` *FAILED* after {elapsed / 60:.1f} minutes",
        f"*Phases complete:* {phases}",
        f"*Error:* {error_message}",
        f"*Error Log File:* {log_filename}",
    ]
    if failed_downloads:
        lines.append(f"*Failed downloads:* {', '.join(failed_downloads)}")
    if failed_tables:
        lines.append(f"*Failed tables:* {', '.join(failed_tables)}")

    send_slack(log, "\n".join(lines), is_error=True)
    send_slack(log, "\n".join(lines), is_error=True)


def slack_file_size_failure(all_checks, failed_checks, log):
    lines = [
        "File size pre-check failed — ETL aborted before any download.",
        "",
        "*File size comparison:*",
        "```",
        f"{'File':<40} {'Now (KB)':>10} {'Prev (KB)':>10} {'Change':>8}  Status",
        "-" * 78,
    ]
    for c in all_checks:
        status = "FAIL ✗" if c["failed"] else "ok"
        lines.append(
            f"{c['file']:<40} {c['current_kb']:>10.1f} "
            f"{c['previous_kb']:>10.1f} {c['pct_change']:>+7.1f}%  {status}"
        )
    lines.append("```")
    lines.append(f"*{len(failed_checks)} file(s) failed the 10% threshold.*")
    send_slack(log, "\n".join(lines), is_error=True)


def slack_phase_failure(phase_name, detail, log):
    """Send an alert for a specific phase failure."""
    send_slack(log, f"Phase failure in *{phase_name}*: {detail}", is_error=True)


def slack_snapshot_failure(error_message, log):
    """Send an alert for snapshot failure (run status unaffected)."""
    send_slack(
        log,
        f"Monthly snapshot failed (run status unaffected): {error_message}",
        is_error=True,
    )
