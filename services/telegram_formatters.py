from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

LOCAL_TZ = ZoneInfo("Europe/Moscow")

from contracts.bot_spec import RobotSpec
from services.quiet_windows import QuietWindowsService
from contracts.robot_registry import format_robot_specs_for_log


def _format_dt_utc(dt) -> str:
    if dt.tzinfo is None:
        dt_utc = dt.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC+0")


def _format_dt_local(dt_utc: datetime, tz: ZoneInfo) -> str:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    dt_local = dt_utc.astimezone(tz)
    return dt_local.strftime("%Y-%m-%d %H:%M:%S")


def _format_weekdays(weekdays: set[int] | None) -> str:
    if not weekdays:
        return "all"
    names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    return ",".join(names[i] for i in sorted(weekdays) if 0 <= i <= 6)


def _format_quiet_rule_for_log(rule: dict, now_utc: datetime) -> str:
    kind = str(rule.get("rule_kind") or "?")
    tz_name = str(rule.get("tz") or "UTC")
    tz = ZoneInfo(tz_name)

    reason = rule.get("reason") or "n/a"
    note = rule.get("note") or ""
    weekdays = rule.get("weekdays")

    blocks: list[str] = []
    if rule.get("block_entries"):
        blocks.append("entry")
    if rule.get("block_exits"):
        blocks.append("exit")
    blocks_s = ",".join(blocks) if blocks else "none"

    if kind == "daily":
        start_t = rule.get("start_time")
        end_t = rule.get("end_time")
        if not start_t or not end_t:
            s = f"daily (tz={tz_name}) [blocks={blocks_s}] reason={reason}"
            return s + (f" note={note}" if note else "")

        # Для пересчёта локального времени берём "сегодня" в TZ правила.
        dt_ref = now_utc.astimezone(tz)
        start_dt = datetime.combine(dt_ref.date(), start_t, tzinfo=tz)
        end_dt = datetime.combine(dt_ref.date(), end_t, tzinfo=tz)
        if end_dt <= start_dt:
            end_dt = end_dt + timedelta(days=1)

        start_local = start_dt.astimezone(LOCAL_TZ)
        end_local = end_dt.astimezone(LOCAL_TZ)

        s = (
            f"daily {start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')} (биржа/{tz_name}) / "
            f"{start_local.strftime('%H:%M')}-{end_local.strftime('%H:%M')} (локальное/MSK) "
            f"[weekdays={_format_weekdays(weekdays)}; blocks={blocks_s}; reason={reason}]"
        )
        return s + (f" note={note}" if note else "")

    if kind == "once":
        start_utc = rule.get("start_utc")
        end_utc = rule.get("end_utc")
        if not start_utc or not end_utc:
            s = f"once (tz={tz_name}) [blocks={blocks_s}] reason={reason}"
            return s + (f" note={note}" if note else "")

        start_ex = start_utc.astimezone(tz)
        end_ex = end_utc.astimezone(tz)
        start_local = start_utc.astimezone(LOCAL_TZ)
        end_local = end_utc.astimezone(LOCAL_TZ)

        s = (
            f"once {start_ex.strftime('%Y-%m-%d %H:%M')}-{end_ex.strftime('%Y-%m-%d %H:%M')} (биржа/{tz_name}) / "
            f"{start_local.strftime('%Y-%m-%d %H:%M')}-{end_local.strftime('%Y-%m-%d %H:%M')} (локальное/MSK) "
            f"[blocks={blocks_s}; reason={reason}]"
        )
        return s + (f" note={note}" if note else "")

    s = f"{kind} (tz={tz_name}) [blocks={blocks_s}] reason={reason}"
    return s + (f" note={note}" if note else "")


def format_quiet_windows_for_robot(
        *,
        robot_id: str,
        quiet_service: QuietWindowsService,
        now_utc: datetime,
) -> str:
    rules = quiet_service.get_enabled_rules_for_robot(
        robot_id=robot_id,
        now_utc=now_utc,
        include_global=True,
    )
    if not rules:
        return ""

    lines = ["quiet_windows:"]
    for r in rules:
        lines.append("  - " + _format_quiet_rule_for_log(r, now_utc))
    return "\n".join(lines)


def _startup_registry_message(
        all_specs: list[RobotSpec],
        enabled_specs: list[RobotSpec],
        ops_db_path: str,
        quiet_service: QuietWindowsService,
) -> str:
    # Variant B: всё важное — в одном стартовом сообщении (без дублей "на каждый робот").
    enabled_ids = ", ".join([s.robot_id for s in enabled_specs]) if enabled_specs else "none"
    now_utc = datetime.now(timezone.utc)

    lines = [
        "IB-робот: запуск.",
        f"robots_enabled: {len(enabled_specs)} ({enabled_ids})",
        "",
        "Реестр (все):",
        format_robot_specs_for_log(all_specs),
    ]

    if enabled_specs and len(enabled_specs) != len(all_specs):
        lines.append("")
        lines.append("Активные (enabled):")
        lines.append(format_robot_specs_for_log(enabled_specs))

    # Окна тишины — только если правила реально есть.
    if enabled_specs:
        quiet_blocks: list[str] = []
        for s in enabled_specs:
            qw = format_quiet_windows_for_robot(
                robot_id=s.robot_id,
                quiet_service=quiet_service,
                now_utc=now_utc,
            )
            if qw:
                quiet_blocks.append(f"robot_id={s.robot_id}\n{qw}")

        if quiet_blocks:
            lines.append("")
            lines.append("Окна тишины (enabled):\n")
            lines.extend(quiet_blocks)

    return "\n".join(lines)
