"""Robot spec for pattern_pirson.

This module is the single source of truth for:
  - robot_id
  - active future contract symbol
  - trade quantity
  - IB orderRef
  - patterns.db instrument_root
  - strategy factory
"""

from contracts.bot_spec import RobotSpec
from .strategy import PatternStrategy


def get_robot_spec() -> RobotSpec:
    robot_id = "pattern_pirson"

    # Текущий активный фьючерс (таблица в price.db и контракт в IB)
    # В будущем это будет вычисляться общим roll-service.
    active_future_symbol = "MNQZ5"

    # Root инструмента в patterns.db
    instrument_root = "MNQ"

    trade_qty = 1

    # Базовая метка ордеров в IB (orderRef)
    order_ref = robot_id

    return RobotSpec(
        robot_id=robot_id,
        active_future_symbol=active_future_symbol,
        instrument_root=instrument_root,
        trade_qty=trade_qty,
        order_ref=order_ref,
        strategy_factory=lambda: PatternStrategy(instrument_root),
    )
