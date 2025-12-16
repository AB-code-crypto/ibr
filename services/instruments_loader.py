"""Instrument universe builder for enabled robots.

Goal:
- Keep robot.py as an orchestrator (event loop + tasks).
- Centralize how we resolve RobotSpec -> IB contracts + InstrumentConfig.
- Allow running the same strategy multiple times (different RobotSpec params) without changing robot.py.

Current convention:
- RobotSpec.active_future_symbol is the *code* used for price.db table name and as a key into futures_for_history.
  Example: "MNQZ5"
- RobotSpec.instrument_root is used by strategies (patterns.db) and also as the IB Future symbol root (e.g. "MNQ").
  If later you need to decouple these (different roots for patterns vs IB), introduce a dedicated field
  (e.g. ib_symbol_root) and update only this module.
"""

from dataclasses import dataclass
from datetime import timedelta

from ib_insync import Future

from contracts.bot_spec import RobotSpec
from core.price_get import InstrumentConfig


@dataclass(frozen=True, slots=True)
class InstrumentsPlan:
    """Resolved market-data universe for enabled robots."""

    # One config per unique active_future_symbol
    instrument_configs: list[InstrumentConfig]

    # Lookup by active_future_symbol (same key as InstrumentConfig.name)
    cfg_by_symbol: dict[str, InstrumentConfig]


def build_instruments_plan(
    *,
    enabled_specs: list[RobotSpec],
    futures_for_history: dict[str, dict],
    exchange: str = "CME",
    currency: str = "USD",
    history_lookback: timedelta = timedelta(days=1),
) -> InstrumentsPlan:
    """Build InstrumentConfig list + lookup map for enabled robots.

    Fail-fast rules:
    - Each enabled RobotSpec.active_future_symbol must exist in futures_for_history.
    - For a given active_future_symbol, all RobotSpec.instrument_root must match (otherwise we can't build one contract).
    - futures_for_history[<symbol>]['contract_month'] must be 'YYYYMM'.
    """

    if not enabled_specs:
        return InstrumentsPlan(instrument_configs=[], cfg_by_symbol={})

    active_symbols = sorted({s.active_future_symbol for s in enabled_specs})

    # Map: active_future_symbol -> IB futures root symbol (today it's spec.instrument_root)
    symbol_to_root: dict[str, str] = {}
    for spec in enabled_specs:
        existing = symbol_to_root.get(spec.active_future_symbol)
        if existing is None:
            symbol_to_root[spec.active_future_symbol] = spec.instrument_root
        elif existing != spec.instrument_root:
            raise ValueError(
                "Conflict: same active_future_symbol used with different instrument_root. "
                f"symbol={spec.active_future_symbol!r}, roots={existing!r} vs {spec.instrument_root!r}"
            )

    instrument_configs: list[InstrumentConfig] = []

    for symbol_code in active_symbols:
        meta = futures_for_history[symbol_code]  # fail-fast: must exist in config
        contract_month = meta["contract_month"]  # fail-fast: must exist

        if not isinstance(contract_month, str) or len(contract_month) != 6 or not contract_month.isdigit():
            raise ValueError(
                f"Invalid contract_month for {symbol_code!r}: {contract_month!r} (expected 'YYYYMM')."
            )

        contract = Future(
            symbol=symbol_to_root[symbol_code],
            lastTradeDateOrContractMonth=contract_month,
            exchange=exchange,
            currency=currency,
        )

        cfg = InstrumentConfig(
            name=symbol_code,
            contract=contract,
            history_lookback=history_lookback,
            history_start=meta.get("history_start"),
            expiry=meta.get("expiry"),
        )
        instrument_configs.append(cfg)

    cfg_by_symbol = {cfg.name: cfg for cfg in instrument_configs}

    return InstrumentsPlan(
        instrument_configs=instrument_configs,
        cfg_by_symbol=cfg_by_symbol,
    )