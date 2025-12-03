"""
Утилита для поиска "дыр" в истории 5-секундных баров.

Логика:
- Берём список фьючерсов из futures_for_history.
- Для каждого инструмента проверяем:
    - существует ли таблица с таким именем в БД;
    - есть ли в ней данные.
- Если данные есть — идём по временным меткам (time_utc) по порядку и
  ищем разрывы больше expected_step_sec (по умолчанию 5 секунд).
- Для каждого разрыва формируем интервалы отсутствующих баров.

Фильтрация "не-дыр" (штатные рыночные перерывы):
- свечи имеют шаг 5 секунд, поэтому реальная дыра с длительностью
  ровно N часов крайне маловероятна;
- если при этом такая "дыра":
    * длится ровно целое количество часов (N * 3600 секунд), и
    * начинается ровно в HH:00:00 (UTC) ИЛИ заканчивается ровно
      в HH:59:55 (UTC),
  то считаем её штатным рыночным перерывом (клиринг, выходные, праздники)
  и НЕ считаем дырой.

Дополнительно:
- отдельным правилом считаем штатным перерывом большой праздничный интервал
  на 37980 баров (≈52.75 часа) с окончанием на ..:59:55.
"""

import sqlite3
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Optional

from core.config import PRICE_DB_PATH, futures_for_history

# Интервал дыры в виде (start_ts, end_ts) — обе границы в секундах UNIX time (UTC)
GapInterval = Tuple[int, int]


def to_unix_ts(value) -> int:
    """
    Преобразование значения из БД в целочисленный UNIX timestamp (секунды).

    Поддерживает:
    - int / float (считаем, что это уже UNIX-время);
    - str с датой/временем (ожидаем формат 'YYYY-MM-DD HH:MM:SS').

    При строках считаем, что время в UTC.
    """
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)

    if isinstance(value, str):
        # В схеме time_utc хранится как 'YYYY-MM-DD HH:MM:SS' (UTC).
        try:
            dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            # На всякий случай пробуем ISO-формат, если когда-то что-то поменяется.
            try:
                dt = datetime.fromisoformat(value.replace(" ", "T"))
            except Exception:
                raise ValueError(
                    f"Не удалось распарсить дату/время из строки: {value!r}"
                ) from exc

        dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())

    raise TypeError(f"Неизвестный тип для time_utc: {type(value)!r}, значение: {value!r}")


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Проверяем, есть ли таблица с заданным именем в SQLite."""
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
        (table_name,),
    )
    return cur.fetchone() is not None


def is_scheduled_gap(
    start_missing_ts: int,
    end_missing_ts: int,
    step_sec: int,
) -> bool:
    """
    Общий фильтр "штатных" перерывов.

    Правило:
    - длительность дыры (по количеству недостающих баров) кратна часу, и
    - интервал начинается ровно в HH:00:00 (UTC)
      ИЛИ заканчивается ровно в HH:59:55 (UTC).

    Дополнительно:
    - отдельное правило для большого праздничного интервала на 37980 баров
      (≈52.75 часа), который мы считаем не дырой.
    """
    # Кол-во недостающих баров в интервале [start_missing_ts, end_missing_ts].
    missing_bars = (end_missing_ts - start_missing_ts) // step_sec + 1
    if missing_bars <= 0:
        return False

    start_dt = datetime.fromtimestamp(start_missing_ts, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_missing_ts, tz=timezone.utc)

    # Специальный case: длинный праздничный интервал ~52.75 часа.
    # Для MNQZ5 это 37980 баров, конец на ..:59:55.
    if missing_bars == 37980 and end_dt.minute == 59 and end_dt.second == 55:
        return True

    duration_sec = missing_bars * step_sec

    # Требование 1: длительность кратна часу (очень маловероятно для "реальной" дыры).
    if duration_sec % 3600 != 0:
        return False

    # Требование 2: начало на ровном часе или конец на 59:55.
    starts_on_hour = (start_dt.minute == 0 and start_dt.second == 0)
    ends_before_hour = (end_dt.minute == 59 and end_dt.second == 55)

    return starts_on_hour or ends_before_hour


def scan_instrument_gaps(
    conn: sqlite3.Connection,
    symbol: str,
    expected_step_sec: int = 5,
) -> Optional[Tuple[List[GapInterval], int, int, int]]:
    """
    Сканирует одну таблицу (инструмент) на предмет дыр в истории.

    Параметры:
        conn              - открытое соединение с SQLite
        symbol            - имя инструмента (и таблицы), например 'MNQM5'
        expected_step_sec - ожидаемый шаг между барами в секундах (по умолчанию 5)

    Возвращает:
        (gaps, first_ts, last_ts, bars_count) или None, если данных нет.

        gaps       - список интервалов пропусков [(start_ts, end_ts), ...]
        first_ts   - метка времени первого бара (UNIX seconds)
        last_ts    - метка времени последнего бара (UNIX seconds)
        bars_count - всего баров в таблице

    Если таблица пустая — возвращает None.
    """
    # Имя таблицы экранируем кавычками, т.к. это идентификатор.
    sql = f'SELECT time_utc FROM "{symbol}" ORDER BY time_utc ASC;'
    cur = conn.execute(sql)

    first_row = cur.fetchone()
    if first_row is None:
        # Данных нет
        return None

    first_ts = to_unix_ts(first_row[0])
    last_ts = first_ts
    bars_count = 1

    gaps: List[GapInterval] = []

    for row in cur:
        ts = to_unix_ts(row[0])
        bars_count += 1

        dt = ts - last_ts
        if dt > expected_step_sec:
            # Есть разрыв больше ожидаемого шага.
            # Предполагаем регулярную сетку: 5с, значит отсутствующие бары —
            # это последовательность меток last_ts + step, ..., ts - step.
            start_missing = last_ts + expected_step_sec
            end_missing = ts - expected_step_sec

            if start_missing <= end_missing:
                # Если разрыв подпадает под наше правило "штатного" перерыва —
                # НЕ считаем его дырой.
                if not is_scheduled_gap(
                    start_missing, end_missing, step_sec=expected_step_sec
                ):
                    gaps.append((start_missing, end_missing))

        last_ts = ts

    return gaps, first_ts, last_ts, bars_count


def merge_gaps(
    gaps: List[GapInterval],
    step_sec: int = 5,
) -> List[GapInterval]:
    """
    На случай, если интервалы пересекаются или соприкасаются,
    объединим их в непрерывные.

    В типичной схеме "из разности соседних баров" интервалы не пересекаются,
    но с учётом возможных артефактов/правок БД добавить merge не повредит.
    """
    if not gaps:
        return []

    gaps_sorted = sorted(gaps, key=lambda g: g[0])
    merged: List[GapInterval] = [gaps_sorted[0]]

    for start, end in gaps_sorted[1:]:
        last_start, last_end = merged[-1]

        # Если новый интервал начинается до или близко к окончанию предыдущего
        # (учитываем шаг), объединяем их.
        if start <= last_end + step_sec:
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))

    return merged


def format_gap(
    start_ts: int,
    end_ts: int,
    step_sec: int = 5,
) -> str:
    """
    Форматируем интервал дыры в удобочитаемый текст:
    - даты/время в ISO (UTC),
    - количество недостающих баров.
    """
    start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ts, tz=timezone.utc)

    # Кол-во недостающих баров считаем по шагу.
    missing_bars = (end_ts - start_ts) // step_sec + 1

    return (
        f"{start_dt.isoformat()} -> {end_dt.isoformat()}  "
        f"| missing bars: {missing_bars}"
    )


def scan_all_instruments(expected_step_sec: int = 5) -> None:
    """
    Основная точка входа: пробегаемся по всем инструментам из futures_for_history,
    ищем дыры (кроме "штатных" перерывов по нашему правилу) и печатаем сводку
    в консоль.
    """
    print("Подключаемся к БД цен:", PRICE_DB_PATH)
    conn = sqlite3.connect(str(PRICE_DB_PATH))

    try:
        all_gaps: Dict[str, List[GapInterval]] = {}
        stats: Dict[str, Dict[str, int]] = {}

        for symbol in futures_for_history.keys():
            print("\n" + "=" * 80)
            print(f"Инструмент: {symbol}")

            if not table_exists(conn, symbol):
                print(f"[{symbol}] Таблицы в БД не найдено — пропускаем.")
                continue

            result = scan_instrument_gaps(
                conn=conn,
                symbol=symbol,
                expected_step_sec=expected_step_sec,
            )

            if result is None:
                print(f"[{symbol}] В таблице нет ни одного бара — пропускаем.")
                continue

            gaps, first_ts, last_ts, bars_count = result
            merged_gaps = merge_gaps(gaps, step_sec=expected_step_sec)

            stats[symbol] = {
                "bars_count": bars_count,
                "gaps_count": len(merged_gaps),
            }

            if not merged_gaps:
                print(
                    f"[{symbol}] Дыр в истории (после фильтра штатных перерывов) не обнаружено "
                    f"(баров: {bars_count})."
                )
                continue

            all_gaps[symbol] = merged_gaps

            first_dt = datetime.fromtimestamp(first_ts, tz=timezone.utc)
            last_dt = datetime.fromtimestamp(last_ts, tz=timezone.utc)

            print(
                f"[{symbol}] Найдено дыр (после фильтра штатных перерывов): {len(merged_gaps)}, "
                f"всего баров: {bars_count}, "
                f"первый бар: {first_dt.isoformat()}, "
                f"последний бар: {last_dt.isoformat()}"
            )

            print("Детализация по интервалам:")
            for idx, (start, end) in enumerate(merged_gaps, start=1):
                print(f"  {idx:3d}. {format_gap(start, end, step_sec=expected_step_sec)}")

        # Итоговый сводный список дыр по всем инструментам
        print("\n" + "#" * 80)
        print("# ИТОГОВЫЙ СПИСОК ДЫР ПО ВСЕМ ИНСТРУМЕНТАМ "
              "(С УЧЁТОМ ФИЛЬТРА ШТАТНЫХ ПЕРЕРЫВОВ)")
        print("#" * 80 + "\n")

        if not all_gaps:
            print("Дыр в истории ни по одному инструменту не найдено.")
            return

        for symbol, gaps in all_gaps.items():
            total_missing_bars = sum(
                (end - start) // expected_step_sec + 1
                for start, end in gaps
            )

            print(f"Инструмент {symbol}:")
            print(f"  Всего дыр: {len(gaps)}")
            print(f"  Оценочно недостающих баров: {total_missing_bars}")

            for idx, (start, end) in enumerate(gaps, start=1):
                print(f"  {idx:3d}. {format_gap(start, end, step_sec=expected_step_sec)}")

            print()

    finally:
        conn.close()


if __name__ == "__main__":
    # Для 5-секундных баров expected_step_sec = 5.
    scan_all_instruments(expected_step_sec=5)
