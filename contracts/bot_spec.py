"""Robot specification and strategy signal interface.

This module defines a minimal contract between the root robot runner (orchestrator)
and pluggable robot implementations (strategy packages).

Note:
  - Keep this file dependency-light. It must be importable by both runner and strategies.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, Optional, Protocol


class StrategySignal(Protocol):
    action: str           # "long" | "short" | "flat"
    reason: str

    # Optional diagnostic fields (used by pattern_pirson; other strategies may omit or set None)
    cluster_db_id: Optional[int]
    similarity: Optional[float]
    slot_sec: Optional[int]
    stats: Optional[Dict[str, float]]
    now_time_utc: Optional[datetime]


class Strategy(Protocol):
    def evaluate_for_contract(self, contract: str) -> StrategySignal: ...


@dataclass(frozen=True, slots=True)
class RobotSpec:
    """Static configuration for a robot instance."""

    robot_id: str

    # For now: one active contract per robot.
    # Later this may evolve into a roll-service / multi-contract setup.
    active_future_symbol: str

    # Strategy-specific instrument root for patterns.db (e.g. "MNQ")
    instrument_root: str

    trade_qty: int
    order_ref: str

    # Create a fresh strategy instance for this robot
    strategy_factory: Callable[[], Strategy]
