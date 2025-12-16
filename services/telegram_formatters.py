from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from contracts.bot_spec import RobotSpec
from services.quiet_windows import QuietWindowsService

# Локальное время пользователя (Москва)
LOCAL_TZ = ZoneInfo("Europe/Moscow")


def _format_dt_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt_utc = dt.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%d %H:%M:%S UTC+0")


def _format_dt_local(dt_utc: datetime, tz: ZoneInfo) -> str:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return dt_utc.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")


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
            return f"daily (tz={tz_name}) [blocks={blocks_s}] reason={reason}"

        # Для пересчёта локального времени на сегодня берём "сегодня" в TZ правила.
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
        if note:
            s += f" note={note}"
        return s

    if kind == "once":
        start_utc = rule.get("start_utc")
        end_utc = rule.get("end_utc")
        if not start_utc or not end_utc:
            return f"once (tz={tz_name}) [blocks={blocks_s}] reason={reason}"

        start_ex = start_utc.astimezone(tz)
        end_ex = end_utc.astimezone(tz)
        start_local = start_utc.astimezone(LOCAL_TZ)
        end_local = end_utc.astimezone(LOCAL_TZ)

        s = (
            f"once {start_ex.strftime('%Y-%m-%d %H:%M')}-{end_ex.strftime('%Y-%m-%d %H:%M')} (биржа/{tz_name}) / "
            f"{start_local.strftime('%Y-%m-%d %H:%M')}-{end_local.strftime('%Y-%m-%d %H:%M')} (локальное/MSK) "
            f"[blocks={blocks_s}; reason={reason}]"
        )
        if note:
            s += f" note={note}"
        return s

    # неизвестный тип правила
    s = f"{kind} (tz={tz_name}) [blocks={blocks_s}] reason={reason}"
    if note:
        s += f" note={note}"
    return s


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
        lines.append(_format_quiet_rule_for_log(r, now_utc))
    return "\n".join(lines)


def build_robot_started_message(
        *,
        spec: RobotSpec,
        quiet_service: QuietWindowsService,
        now_utc: datetime,
) -> str:
    msg = (
        f"[{spec.robot_id}] Запущен.\n"
        f"instrument: {spec.active_future_symbol}\n"
        f"volume: {spec.trade_qty}\n"
        f"order_ref: {spec.order_ref}"
    )

    quiet_block = format_quiet_windows_for_robot(
        robot_id=spec.robot_id,
        quiet_service=quiet_service,
        now_utc=now_utc,
    )
    if quiet_block:
        msg += "\n" + quiet_block

    return msg


def _startup_registry_message(
        all_specs: list[RobotSpec],
        enabled_specs: list[RobotSpec],
        ops_db_path: str,
        quiet_service: QuietWindowsService,
) -> str:
    now_utc = datetime.now(timezone.utc)

    enabled_ids = ", ".join([s.robot_id for s in enabled_specs]) if enabled_specs else "none"

    lines: list[str] = [
        "IB-робот: запуск.",
        f"robots_registered: {len(all_specs)}",
        f"robots_enabled: {len(enabled_specs)} ({enabled_ids})",
    ]

    return "\n".join(lines)
