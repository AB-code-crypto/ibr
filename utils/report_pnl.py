import asyncio
import logging
from datetime import datetime
from typing import List

from ib_insync import Trade
import pandas as pd

# Импорты из вашего проекта
from core.ib_connect import IBConnect

try:
    from core.config import IB_HOST, IB_PORT
except ImportError:
    IB_HOST = "127.0.0.1"
    IB_PORT = 7496

# Используем отдельный ID, чтобы не конфликтовать с основным роботом
IB_CLIENT_ID = 103

logger = logging.getLogger(__name__)


async def run_report_all_trades():
    """
    Основная логика отчета: запрашиваем ВСЕ доступные сделки
    без фильтрации по дате или метке ордера, и суммируем общий P&L.
    """

    ib_conn = IBConnect(
        host=IB_HOST,
        port=IB_PORT,
        client_id=IB_CLIENT_ID,
        connect_timeout=15.0,
        keepalive_sec=30.0,
    )

    connector_task = asyncio.create_task(ib_conn.run_forever(), name="IB_Connector")

    try:
        logger.info("Connecting to TWS/Gateway...")
        await ib_conn.wait_connected()
        logger.info("Connection established. Requesting ALL trades (approx. last 7-14 days)...")

        # 1. Запрос ВСЕХ сделок (без фильтров!)
        all_trades: List[Trade] = ib_conn.client.trades()

        # 2. Обработка
        report_data = []

        for trade in all_trades:
            # Ищем сделки, у которых есть исполнение и которые закрыты/имеют P&L
            is_filled = trade.orderStatus.filled > 0

            if is_filled:
                # Включаем все сделки с исполнением
                fill = trade.fills[0]  # Берем первое исполнение для сбора данных

                # Собираем данные
                report_data.append({
                    'time_local': fill.time,  # Время исполнения в локальной TZ TWS/Gateway
                    'ref': trade.order.orderRef,
                    'symbol': trade.contract.localSymbol,
                    'action': trade.order.action,
                    'quantity': fill.execution.shares,
                    'pnl': trade.realizedPnl(),
                    'commission': trade.commission(),
                })

        # 3. Агрегация данных
        if not report_data:
            print("\n" + "=" * 50)
            print(f"❌ Trades not found in TWS/Gateway memory.")
            print("   Проверьте TWS: запущен ли он и есть ли сделки за последние 14 дней?")
            print("==================================================\n")
            return

        df = pd.DataFrame(report_data)

        # Заменяем None на 0 для безопасного суммирования
        df['pnl'] = df['pnl'].fillna(0)
        df['commission'] = df['commission'].fillna(0)

        total_pnl = df['pnl'].sum()
        total_commission = df['commission'].sum()
        net_pnl = total_pnl - total_commission

        # Вывод результатов
        print("\n" + "=" * 50)
        print("📈 Total P&L Report (All Available Trades)")
        print("-" * 50)
        print(f"  Total Trades Found (all refs, all dates): {len(df)}")

        # Вывод для проверки: какие рефы были найдены
        all_refs = df['ref'].unique().tolist()
        print(f"  Unique Order Refs Found: {all_refs}")

        print(f"  Realized PnL (Gross): {total_pnl:,.2f} USD")
        print(f"  Total Commission:     {total_commission:,.2f} USD")
        print(f"  Net PnL:              {net_pnl:,.2f} USD")
        print("=" * 50 + "\n")

        # Вывод деталей для проверки дат
        print("--- Detailed Trade List (Top 5) ---")
        detail_df = df[['time_local', 'ref', 'symbol', 'action', 'quantity', 'pnl', 'commission']].head(5)
        print(detail_df.to_markdown(index=False, floatfmt=".2f"))
        print("\n")


    except asyncio.CancelledError:
        logger.info("Report task cancelled.")
        raise
    except Exception as e:
        logger.error("Report execution failed: %s", e, exc_info=True)
    finally:
        await ib_conn.shutdown()
        connector_task.cancel()
        await asyncio.gather(connector_task, return_exceptions=True)


if __name__ == "__main__":
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    try:
        asyncio.run(run_report_all_trades())
    except KeyboardInterrupt:
        print("\nReport interrupted by user.")