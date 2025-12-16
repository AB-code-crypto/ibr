

# IB Robot (ibr) — архитектура и поток выполнения

## 1. Назначение и принципы

Проект реализует асинхронного торгового робота для Interactive Brokers (IB), который:

* подключается к IB (Gateway/TWS),
* поднимает pipeline маркет-данных (история + стрим баров) и пишет их в SQLite (`price.db`),
* запускает один или несколько торговых роботов (экземпляров стратегий) по `RobotSpec`,
* применяет централизованные ограничения “окна тишины” (quiet windows) на вход/выход,
* ведёт телеметрию в Telegram (канал логов и канал трейдинга),
* поддерживает runtime-переключатели роботов (enable/disable) через SQLite (`robot_ops.db`) без перезапуска.

Ключевой принцип: **`robot.py` — только оркестратор** (event loop + wiring + таски). Логика торговли и маркет-данных вынесена в `services/*`. Выбор рабочих инструментов вынесен в промежуточный слой `services/instruments_loader.py`.



## 2. Структура модулей и ответственности

Ниже — “карта” модулей, что в них лежит и кто их вызывает.

### 2.1. Оркестратор

**`robot.py`**

* Единственная точка входа: `async def main()`
* Оркестрация:

  * инициализация БД и сервисов,
  * загрузка спецификаций роботов,
  * определение enabled-роботов,
  * построение плана инструментов,
  * запуск задач (tasks),
  * обработка сигналов остановки,
  * graceful shutdown.

`robot.py` **не** должен:

* собирать IB-контракты руками,
* содержать торговую логику,
* форматировать телеграм-сообщения,
* содержать state машинок антиспама.

---

### 2.2. Контракты между runner и стратегиями

**`contracts/bot_spec.py`**

* `RobotSpec` — статическая спецификация экземпляра робота:

  * `robot_id`
  * `active_future_symbol` — ключ инструмента (используется как имя таблицы в `price.db` и ключ в `futures_for_history`)
  * `instrument_root` — корень инструмента (пример: `MNQ`), используется стратегией и при сборке IB-контракта
  * `trade_qty`
  * `order_ref`
  * `strategy_factory` — фабрика, создающая новый экземпляр стратегии
* `Strategy` (Protocol): `evaluate_for_contract(contract: str) -> StrategySignal`
* `StrategySignal` (Protocol): минимум полей

  * `action`: `"long" | "short" | "flat"`
  * `reason`
  * диагностические поля (опционально): `cluster_db_id`, `similarity`, `slot_sec`, `stats`, `now_time_utc`

Это **контракт**: runner не знает деталей стратегии, он умеет только вызвать `evaluate_for_contract()` и получить сигнал.

---

### 2.3. Реестр роботов и runtime-переключатели

**`contracts/robot_registry.py`**

* `ROBOT_SPEC_FACTORIES`: `robot_id -> factory() -> RobotSpec`
* `load_all_robot_specs()`:

  * строит список `RobotSpec` для всех зарегистрированных роботов,
  * валидирует соответствие `robot_id` ключу реестра,
  * валидирует уникальность `robot_id`.
* `format_robot_specs_for_log(specs)` — форматирование списка спецификаций для логов/старта.
* `RobotSwitchesStore` — хранение enabled/disabled флага в `robot_ops.db`:

  * `ensure_schema()` — создаёт таблицу `robot_switches`
  * `seed_defaults(robot_ids, default_enabled=True)` — сидирование записей
  * `enabled_robot_ids(all_ids)` — выборка enabled-идентификаторов
  * `is_enabled(robot_id)` — проверка в рантайме (используется trading loop)

**Таблица** `robot_switches` (в `robot_ops.db`) обеспечивает внешнее управление “включён/выключен” без правок кода.

---

### 2.4. Промежуточный слой: план рабочих инструментов

**`services/instruments_loader.py`**

* Решает проблему: *оркестратор не должен знать, как собирать Future/InstrumentConfig*.
* Главная функция: `build_instruments_plan(...) -> InstrumentsPlan`, где:

  * вход: `enabled_specs: list[RobotSpec]` + `futures_for_history: dict`
  * выход:

    * `instrument_configs: list[InstrumentConfig]` — уникальный список конфигов для маркет-данных
    * `cfg_by_symbol: dict[str, InstrumentConfig]` — доступ по `active_future_symbol`

Проверки “fail fast”:

* каждый `RobotSpec.active_future_symbol` обязан присутствовать в `futures_for_history`,
* если один и тот же `active_future_symbol` используется разными роботами — `instrument_root` должен совпадать (иначе невозможно собрать один контракт),
* `contract_month` должен быть `YYYYMM`.

Итог: **market-data pipeline и trading pipeline гарантированно работают на одном и том же universe**.

---

### 2.5. Маркет-данные: история + стрим

**`services/price_streaming_runtime.py`**

* `price_streamer_loop(ib, db, cfg, stop_event)`

  * следит за соединением с IB,
  * делает “gap backfill” перед стримом: `PriceCollector.sync_history_for(cfg, ...)`,
  * запускает стрим баров: `PriceStreamer.stream_bars(cfg, ...)`,
  * при ошибках перезапускает цикл с паузой,
  * корректно останавливается по `stop_event`.

**`core/price_get.py` (интерфейсы, используемые оркестратором и runtime)**

* `InstrumentConfig` — описание инструмента для данных/стрима
* `PriceCollector`:

  * `sync_many(instrument_configs, ...)` — первичная загрузка истории на старте
  * `sync_history_for(cfg, ...)` — дозагрузка “дыр” по конкретному инструменту
* `PriceStreamer`:

  * `stream_bars(cfg, ...)` — подписка на realtime бары и запись в БД

**`core/price_db.py`**

* `PriceDB.connect() / close()`
* `ensure_table(symbol_code)`
* `get_last_bar_datetime(symbol_code)`
* (конвенция проекта: по инструменту — отдельная таблица, имя таблицы = `active_future_symbol`)

---

### 2.6. Торговый runtime

**`services/trading_runtime.py`**

* `trading_loop(...)` — главный торговый цикл одного робота
* `_QuietState` — runtime-состояние уведомлений по quiet windows (анти-дубли/антиспам); живёт **только** здесь, не в `robot.py`.

Основные обязанности `trading_loop`:

* регулярно проверять `switches.is_enabled(robot_id)`:

  * если робот отключён и позиции нет — не входить в сделки;
  * если позиция есть — продолжать управлять выходом (чтобы робот мог корректно “дожить позицию”).
* получать торговый сигнал: `strategy.evaluate_for_contract(cfg.name)`
* применять quiet windows:

  * `quiet_service.evaluate(..., action_type="entry")` перед входом
  * `quiet_service.evaluate(..., action_type="exit")` перед выходом
* управлять жизненным циклом позиции:

  * вход: `trade_engine.market_order(...)`
  * выход: логика выхода + fallback при длительной блокировке выхода (grace-период)
* отправлять торговые сообщения в `tg_trading`, а эксплуатационные — в `tg_common`.

**`core/trade_engine.py`**

* `TradeEngine.market_order(contract, action, quantity, expected_price, timeout, order_ref)` — фактическая отправка ордера в IB.

---

### 2.7. Quiet windows (окна тишины)

**`services/quiet_windows.py`**

* `QuietWindowsService` — централизованный фильтр quiet windows, хранит правила в `robot_ops.db`.
* `evaluate(robot_id, now_utc, action_type)` → `QuietDecision(allowed, reason, rule_id)`
* `get_enabled_rules_for_robot(robot_id, now_utc, include_global=True)` — используется для формирования стартового сообщения/диагностики.
* `seed_default_rth_open(robot_id)` — сидирует базовое правило (если правил нет).

**Таблица** `quiet_windows` (в `robot_ops.db`) поддерживает:

* `rule_kind`: `daily` / `once`
* TZ-правила для `daily` (`tz`, `weekdays`, `start_time`, `end_time`)
* UTC-интервал для `once` (`start_utc`, `end_utc`)
* флаги блокировки: `block_entries`, `block_exits`
* `robot_id` конкретный или `'*'` для глобальных правил

---

### 2.8. Telegram сообщения

**`services/telegram_formatters.py`**

* `_startup_registry_message(all_specs, enabled_specs, ops_db_path, quiet_service)`
  Реализован **вариант B**: *всё важное в одном стартовом сообщении*:

  * сколько роботов зарегистрировано,
  * сколько enabled и какие `robot_id`,
  * параметры enabled-роботов (через `format_robot_specs_for_log`),
  * quiet windows (только если реально есть правила).

Дополнительно форматтеры времени:

* `_format_dt_utc(dt)`
* `_format_dt_local(dt_utc, tz)` (для сообщений “по бирже” и “локально”)

---

## 3. Поток выполнения после запуска

Ниже описан “путь” от запуска процесса до торговли и мониторинга.

### 3.1. Старт (`robot.py: main()`)

1. **Поднимается БД цен**

* `db = PriceDB(PRICE_DB_PATH)`
* `await db.connect()`

2. **Поднимается ops DB (robot_ops.db)**

* `ops_db_path = ROBOT_OPS_DB_PATH`

3. **Загрузка всех спецификаций**

* `all_specs = load_all_robot_specs()`

4. **Переключатели роботов**

* `switches = RobotSwitchesStore(db_path=ops_db_path, ...)`
* `switches.ensure_schema()`
* `switches.seed_defaults(known_robot_ids, default_enabled=True)`
* `enabled_specs = [spec for spec in all_specs if spec.robot_id in switches.enabled_robot_ids(...)]`

5. **Quiet windows**

* `quiet_service = QuietWindowsService(db_path=ops_db_path, ...)`
* `quiet_service.ensure_schema()`
* `quiet_service.seed_default_rth_open(robot_id=...)` для каждого робота

6. **IB соединение**

* `ib = IBConnect(host="127.0.0.1", port=7496, client_id=101)`
* создаётся task: `asyncio.create_task(ib.run_forever(), ...)`
* далее ожидание: `await ib.wait_connected(timeout=...)`

7. **Telegram**

* `tg_common = TelegramChannel(bot_token=..., chat_id=TELEGRAM_CHAT_ID_COMMON)`
* `tg_trading = TelegramChannel(bot_token=..., chat_id=TELEGRAM_CHAT_ID_TRADING)`

8. **Единое стартовое сообщение (Variant B)**

* `await tg_common.send(_startup_registry_message(..., quiet_service))`

9. **Сигналы остановки**

* `stop_event = asyncio.Event()`
* `SIGINT/SIGTERM` → `stop_event.set()`

---

### 3.2. Построение universe инструментов

После того как известны `enabled_specs`, оркестратор строит “план инструментов”:

* `plan = build_instruments_plan(enabled_specs=enabled_specs, futures_for_history=futures_for_history, ...)`
* `instrument_configs = plan.instrument_configs` — для данных
* `cfg_by_symbol = plan.cfg_by_symbol` — привязка робота к его `InstrumentConfig`

Именно с этого момента **и history, и streaming, и торговля используют один и тот же список инструментов**.

---

### 3.3. Первичная загрузка истории (backfill) на старте

Оркестратор делает первичную загрузку истории для всех инструментов:

* `collector = PriceCollector(ib=ib.client, db=db)`
* для каждого `cfg`:

  * `await db.ensure_table(cfg.name)`
  * `await db.get_last_bar_datetime(cfg.name)` → сообщение “состояние истории”
* `results = await collector.sync_many(instrument_configs, ...)`
* сообщение “загрузка истории завершена”

Цель: перед стартом торговли иметь актуальные данные в `price.db`.

---

### 3.4. Непрерывный стрим баров

Для каждого `InstrumentConfig` запускается отдельная задача:

* `asyncio.create_task(price_streamer_loop(ib, db, cfg, stop_event), ...)`

`price_streamer_loop`:

* ждёт соединение,
* перед стримом делает дозагрузку дыр (`sync_history_for`),
* затем `stream_bars`,
* при ошибках делает ретраи.

---

### 3.5. Запуск торговых циклов

Для каждого `enabled_spec`:

* `strategy = spec.strategy_factory()`
* `active_cfg = cfg_by_symbol[spec.active_future_symbol]`
* task: `asyncio.create_task(trading_loop(..., strategy=strategy, cfg=active_cfg, ...), ...)`

`trading_loop`:

* читает signal от стратегии (`evaluate_for_contract(cfg.name)`),
* проверяет quiet windows:

  * вход: `evaluate(..., "entry")`
  * выход: `evaluate(..., "exit")`
* при разрешении — исполняет `TradeEngine.market_order(...)`
* пишет сообщения:

  * в `tg_trading` — торговые события (сигнал, вход/выход, результат ордера),
  * в `tg_common` — эксплуатационные события (quiet windows state, блокировки выхода и т.п.)
* учитывает `RobotSwitchesStore.is_enabled(robot_id)` в рантайме.

---

### 3.6. Фоновые мониторинги

Также запускаются:

* `hourly_status_loop(ib, tg_common)` — “heartbeat” раз в час
* `portfolio_monitor_loop(ib, tg_common, stop_event)` — мониторинг портфеля

---

## 4. Остановка и завершение

При `SIGINT/SIGTERM`:

1. выставляется `stop_event`
2. оркестратор:

* отправляет в `tg_common` сообщение о shutdown,
* вызывает `await ib.shutdown()`,
* `cancel()` всех tasks и `await gather(..., return_exceptions=True)`,
* закрывает `TelegramChannel` и `PriceDB`.

---

## 5. Как добавить нового робота / стратегию

1. Создать пакет стратегии: `strategies/<robot_id>/`
2. Внутри — `spec.py` с функцией:

   * `get_robot_spec() -> RobotSpec`
   * заполняет:

     * `robot_id`
     * `active_future_symbol`
     * `instrument_root`
     * `trade_qty`, `order_ref`
     * `strategy_factory`
3. Зарегистрировать фабрику в `contracts/robot_registry.py` в `ROBOT_SPEC_FACTORIES`
4. Убедиться, что инструмент присутствует в `core/config.py:futures_for_history` (метаданные `contract_month`, опционально `history_start`, `expiry`)
5. При необходимости — добавить/изменить quiet windows правила в `robot_ops.db` (таблица `quiet_windows`)

---

## 6. Практические инварианты (что важно не ломать)

* `RobotSpec.active_future_symbol`:

  * ключ инструмента для `futures_for_history`
  * имя таблицы в `price.db`
  * ключ `cfg_by_symbol[...]` в `InstrumentsPlan`
* `RobotSpec.instrument_root`:

  * используется стратегией (например, для patterns.db)
  * используется `instruments_loader` при сборке IB-контракта `Future(symbol=instrument_root, ...)`
* Оркестратор (`robot.py`) **не** содержит:

  * `_QuietState`
  * форматтеры телеги
  * торговые циклы
  * создание `Future(...)` напрямую

---

## 7. Быстрая диагностика (если “что-то не работает”)

* Нет quiet windows в стартовом сообщении:

  * проверить, что отправляется именно `_startup_registry_message(..., quiet_service)` из `services/telegram_formatters.py`
  * проверить таблицу `quiet_windows` в `robot_ops.db` (`enabled=1`, `robot_id` совпадает или `'*'`)
* Робот “не торгует”:

  * проверить `robot_switches.enabled=1` для `robot_id`
  * проверить, что `enabled_specs` не пуст
* Нет данных по инструменту:

  * проверить наличие ключа `active_future_symbol` в `futures_for_history`
  * проверить, что создаётся `InstrumentConfig` и запущена задача `price_streamer_loop`

---
## 8. Sequence: Start / Stop (детально)
Participants:
  OS/CLI | robot.py(main) | PriceDB | opsDB(robot_ops.db) | RobotRegistry | SwitchesStore
  QuietWindowsService | InstrumentsLoader | IBConnect | Telegram(logs) | Telegram(trades)
  PriceCollector | PriceStreamerLoop | TradingRuntime | HourlyStatus | PortfolioMonitor

START
OS/CLI            -> robot.py(main)              : asyncio.run(main)
robot.py(main)    -> PriceDB                     : connect()
robot.py(main)    -> RobotRegistry               : load_all_robot_specs()
RobotRegistry     --> robot.py(main)             : all_specs

robot.py(main)    -> SwitchesStore               : ensure_schema()
robot.py(main)    -> SwitchesStore               : seed_defaults(known_robot_ids)
robot.py(main)    -> SwitchesStore               : enabled_robot_ids(known_robot_ids)
SwitchesStore     --> robot.py(main)             : enabled_ids
robot.py(main)    -> robot.py(main)              : enabled_specs = filter(all_specs, enabled_ids)

robot.py(main)    -> QuietWindowsService         : ensure_schema()
loop(all_specs)
  robot.py(main)  -> QuietWindowsService         : seed_default_rth_open(robot_id)
end

robot.py(main)    -> Telegram(logs)              : init(bot_token, chat_id_common)
robot.py(main)    -> Telegram(trades)            : init(bot_token, chat_id_trading)

robot.py(main)    -> Telegram(logs)              : send(startup_registry_message Variant-B)
Telegram(logs)    <-- QuietWindowsService        : get_enabled_rules_for_robot(...)  [inside formatter]

robot.py(main)    -> IBConnect                   : create(127.0.0.1:7496, clientId=101)
robot.py(main)    -> IBConnect                   : start task run_forever()
robot.py(main)    -> IBConnect                   : wait_connected(timeout)
IBConnect         --> robot.py(main)             : connected? (True/False)

alt connected == False
  robot.py(main)  -> Telegram(logs)              : send("cannot connect")
  robot.py(main)  -> robot.py(main)              : go to shutdown
else connected == True
  robot.py(main)  -> Telegram(logs)              : send(initial_connect_message)

  robot.py(main)  -> InstrumentsLoader           : build_instruments_plan(enabled_specs, futures_for_history)
  InstrumentsLoader --> robot.py(main)            : plan {instrument_configs, cfg_by_symbol}

  robot.py(main)  -> PriceCollector              : init(ib.client, PriceDB)

  loop(plan.instrument_configs)
    robot.py(main)-> PriceDB                     : ensure_table(cfg.name)
    robot.py(main)-> PriceDB                     : get_last_bar_datetime(cfg.name)
  end
  robot.py(main)  -> Telegram(logs)              : send("history state before load" if any)

  robot.py(main)  -> PriceCollector              : sync_many(instrument_configs, chunk_seconds, cancel_event)
  PriceCollector  -> PriceDB                     : insert bars (per symbol)
  PriceCollector  --> robot.py(main)             : results {symbol: inserted}
  robot.py(main)  -> Telegram(logs)              : send("history loaded summary" if any)

  loop(plan.instrument_configs)
    robot.py(main)-> PriceStreamerLoop           : create task price_streamer_loop(ib, db, cfg, stop_event)
  end

  robot.py(main)  -> TradeEngine                 : init(ib.client)

  loop(enabled_specs)
    robot.py(main)-> TradingRuntime              : create task trading_loop(ib, trade_engine, strategy, cfg, tg_*, quiet_service, switches, stop_event)
  end

  robot.py(main)  -> HourlyStatus                : create task hourly_status_loop(ib, tg_logs)
  robot.py(main)  -> PortfolioMonitor            : create task portfolio_monitor_loop(ib, tg_logs, stop_event)
end

RUNNING
... (tasks operate until stop_event is set) ...

STOP (SIGINT/SIGTERM or external)
OS/CLI            -> robot.py(main)              : signal handler sets stop_event
robot.py(main)    -> Telegram(logs)              : send("shutdown")  [best-effort]
robot.py(main)    -> IBConnect                   : shutdown()
robot.py(main)    -> *all tasks*                 : cancel()
robot.py(main)    -> Telegram(logs)              : close()
robot.py(main)    -> Telegram(trades)            : close()
robot.py(main)    -> PriceDB                     : close()
robot.py(main)    --> OS/CLI                     : exit


## 9. Sequence: Startup → Data → Trading → Shutdown (сквозное, компактнее)
Participants:
  robot.py | InstrumentsLoader | PriceDB | IB | MarketData(collector+streamer) | Strategy | QuietWindows | TradeEngine | Telegram(logs) | Telegram(trades)

1) STARTUP
robot.py  -> PriceDB                 : connect()
robot.py  -> Telegram(logs)          : send("startup registry + enabled robots + quiet windows")
robot.py  -> IB                      : connect/run_forever + wait_connected()

2) UNIVERSE / PLAN
robot.py  -> InstrumentsLoader       : build_instruments_plan(enabled_specs, futures_for_history)
InstrumentsLoader --> robot.py       : plan {instrument_configs, cfg_by_symbol}

3) DATA PIPELINE (history + stream)
robot.py  -> MarketData              : sync_many(plan.instrument_configs)  [initial backfill]
MarketData-> PriceDB                 : insert historical bars
robot.py  -> MarketData              : start price_streamer_loop task per cfg
MarketData-> IB                      : subscribe realtime bars
MarketData-> PriceDB                 : append bars continuously

4) TRADING PIPELINE (per enabled robot)
loop for each enabled RobotSpec
  robot.py  -> Strategy              : create instance via strategy_factory()
  robot.py  -> TradingRuntime        : start trading_loop(spec, cfg_by_symbol[spec.active_future_symbol])
end

Within trading_loop (repeated):
Strategy      -> TradingRuntime      : evaluate_for_contract(cfg.name) -> signal(action, reason,...)
TradingRuntime-> QuietWindows        : evaluate(robot_id, now_utc, "entry"/"exit") -> allowed?
alt allowed == False
  TradingRuntime -> Telegram(logs)   : notify state-change / exit-block risk (no spam)
else allowed == True
  TradingRuntime -> TradeEngine      : market_order(contract, side, qty, order_ref)
  TradeEngine    -> IB               : place order + await fill
  TradingRuntime -> Telegram(trades) : send execution result
end

5) SHUTDOWN
robot.py  -> Telegram(logs)          : send("shutdown")
robot.py  -> IB                      : shutdown()
robot.py  -> MarketData              : cancel streamer tasks
robot.py  -> TradingRuntime          : cancel trading tasks
robot.py  -> PriceDB                 : close()
robot.py  -> Telegram(logs/trades)   : close()

## 10. DATAFLOW DIAGRAM (bars → sqlite → strategy → orders → telegram)

Legend:
  [Component]   runtime module/service
  (Storage)     persistent storage
  -->           data/control flow
  ~~>           async stream / continuous flow


                         ┌───────────────────────────┐
                         │        IB Gateway/TWS      │
                         │     (Interactive Brokers)  │
                         └─────────────┬─────────────┘
                                       │
                    historical bars     │ realtime bars
                    (requests/replies)  │ (subscriptions)
                                       │
                                       v
┌─────────────────────────────┐   ~~>  ┌─────────────────────────────┐
│  MarketData: PriceCollector │        │ MarketData: PriceStreamer   │
│  core/price_get.py          │        │ core/price_get.py            │
│  - sync_many()              │        │ - stream_bars()              │
│  - sync_history_for()       │        │                               │
└──────────────┬──────────────┘        └──────────────┬──────────────┘
               │                                       │
               │ inserts bars                           │ appends bars
               v                                       v
        ┌─────────────────────────────────────────────────────┐
        │                    (price.db SQLite)                 │
        │   One table per instrument: table name = symbol_code │
        │   Example: "MNQZ5"                                   │
        └──────────────┬───────────────────────────────────────┘
                       │
                       │ read latest bars / windows / features
                       v
              ┌──────────────────────────────┐
              │        Strategy instance      │
              │   strategies/* (your code)    │
              │   evaluate_for_contract(...)  │
              └──────────────┬───────────────┘
                             │ produces
                             │ StrategySignal(action, reason, ...)
                             v
        ┌──────────────────────────────────────────────────┐
        │              Trading Runtime (services)           │
        │          services/trading_runtime.py              │
        │  - trading_loop()                                 │
        │  - position state, signal handling, order calls    │
        │  - runtime enable/disable (RobotSwitchesStore)     │
        └───────────────┬───────────────────────────────────┘
                        │
                        │ consults rules (entry/exit gating)
                        v
          ┌─────────────────────────────────┐
          │     Quiet Windows Service        │
          │   services/quiet_windows.py      │
          │   evaluate(robot_id, now_utc,    │
          │           action_type=entry/exit)│
          └───────────────┬─────────────────┘
                          │ reads rules
                          v
                   ┌───────────────────────────┐
                   │      (robot_ops.db)        │
                   │  - quiet_windows table     │
                   │  - robot_switches table    │
                   └───────────────────────────┘

If allowed (not in quiet window):
Trading Runtime --> TradeEngine --> IB (place order, await fills)
                 core/trade_engine.py

If blocked:
Trading Runtime --> Telegram(logs)  (state-change / risk messages)
If executed:
Trading Runtime --> Telegram(trades) (fills/executions)

Telegram channels:
  ┌──────────────────────────┐         ┌──────────────────────────┐
  │   Telegram(logs/common)   │         │   Telegram(trades)       │
  │ core/telegram.py          │         │ core/telegram.py          │
  └──────────────────────────┘         └──────────────────────────┘


CONTROL PLANE (configuration / orchestration)

┌───────────────────────────┐
│     robot.py (orchestrator)│
│  - loads RobotSpec registry │
│  - builds instruments plan  │
│  - starts tasks             │
└──────────────┬────────────┘
               │ enabled_specs
               v
   ┌────────────────────────────────┐
   │ Instruments Loader (services)  │
   │ services/instruments_loader.py │
   │ build_instruments_plan(...)    │
   └────────────────────────────────┘
               │ produces plan:
               │  - instrument_configs (for data)
               │  - cfg_by_symbol (for trading)
               v
  MarketData tasks (per InstrumentConfig)  +  Trading tasks (per RobotSpec)

## 11. Словарь сущностей

Ниже — “словарь сущностей” в формате, удобном для README: что это такое, где определяется, кто создаёт, кто потребляет, и какие ключевые поля/инварианты важны.

---

## Словарь сущностей

### 1) `RobotSpec`

* **Где**: `contracts/bot_spec.py` (контракт и dataclass/структура спецификации)
* **Что**: спецификация *экземпляра* робота (один запуск стратегии с конкретными параметрами)
* **Кто создаёт**: фабрика из реестра `contracts/robot_registry.py` (через `ROBOT_SPEC_FACTORIES`)
* **Кто потребляет**:

  * `robot.py` — чтобы сформировать список enabled и запустить `trading_loop`
  * `services/instruments_loader.py` — чтобы построить universe инструментов
* **Ключевые поля (инварианты)**:

  * `robot_id` — уникальный идентификатор запуска
  * `active_future_symbol` — ключ инструмента (используется как имя таблицы в `price.db` и ключ в `futures_for_history`)
  * `instrument_root` — корень инструмента (например, `MNQ`), используется при сборке IB Future и в стратегии
  * `trade_qty`, `order_ref`
  * `strategy_factory()` — создаёт новый инстанс стратегии для этого робота

---

### 2) `RobotRegistry` / `ROBOT_SPEC_FACTORIES`

* **Где**: `contracts/robot_registry.py`
* **Что**: реестр доступных роботов (robot_id → factory → RobotSpec)
* **Кто создаёт**: код проекта (ручная регистрация фабрик)
* **Кто потребляет**: `robot.py` через `load_all_robot_specs()`
* **Ключевое**:

  * гарантирует уникальность `robot_id`
  * централизует “какие роботы вообще существуют”

---

### 3) `RobotSwitchesStore`

* **Где**: `contracts/robot_registry.py`
* **Что**: runtime-переключатель enabled/disabled для `robot_id`
* **Хранилище**: `(robot_ops.db)` таблица `robot_switches`
* **Кто создаёт/инициализирует**: `robot.py` (ensure_schema + seed_defaults)
* **Кто потребляет**:

  * `robot.py` — чтобы вычислить `enabled_specs` на старте
  * `services/trading_runtime.py` — чтобы в цикле учитывать `is_enabled(robot_id)`

---

### 4) `InstrumentsPlan`

* **Где**: `services/instruments_loader.py`
* **Что**: “план инструментов” (universe), связующий слой между RobotSpec и market data
* **Кто создаёт**: `build_instruments_plan(enabled_specs, futures_for_history, ...)`
* **Кто потребляет**: `robot.py`
* **Содержит**:

  * `instrument_configs: list[InstrumentConfig]` — уникальные инструменты для истории/стрима
  * `cfg_by_symbol: dict[active_future_symbol -> InstrumentConfig]` — быстрый доступ для торговых циклов
* **Инвариант**: market-data и trading используют одну и ту же модель инструмента (через plan).

---

### 5) `futures_for_history`

* **Где**: `core/config.py`
* **Что**: конфигурационный словарь метаданных по инструментам (expiry, history_start, contract_month, и т.п.)
* **Кто создаёт**: код проекта (конфиг)
* **Кто потребляет**:

  * `services/instruments_loader.py` — для сборки контрактов/InstrumentConfig
* **Инвариант**: каждый `RobotSpec.active_future_symbol` должен существовать ключом в `futures_for_history`.

---

### 6) `Future` (IB contract)

* **Где**: `ib_insync.Future` (внешняя библиотека)
* **Что**: IB-контракт фьючерса (symbol root + contract month + exchange + currency)
* **Кто создаёт**: `services/instruments_loader.py`
* **Кто потребляет**:

  * `MarketData` (collector/streamer) — чтобы запрашивать/стримить бары
  * `TradeEngine` — чтобы размещать ордера
* **Инвариант**: сборка должна быть централизована (не в robot.py), иначе начнётся дрейф параметров.

---

### 7) `InstrumentConfig`

* **Где**: `core/price_get.py`
* **Что**: конфигурация инструмента для data pipeline (contract + параметры истории)
* **Кто создаёт**: `services/instruments_loader.py`
* **Кто потребляет**:

  * `core/price_get.PriceCollector` (history backfill)
  * `core/price_get.PriceStreamer` (stream bars)
  * `services/trading_runtime.py` (привязка робота к инструменту)
* **Ключевые поля**:

  * `name` — символ-код (обычно `active_future_symbol`), также имя таблицы в `price.db`
  * `contract` — `Future`
  * `history_lookback`, `history_start`, `expiry`

---

### 8) `PriceDB`

* **Где**: `core/price_db.py`
* **Что**: SQLite-хранилище баров
* **Физически**: файл `PRICE_DB_PATH` (SQLite)
* **Кто пишет**:

  * `PriceCollector` (история)
  * `PriceStreamer` (реaltime)
* **Кто читает**:

  * стратегии (напрямую или через свои сервисы/обёртки; по проектной логике стратегия берёт данные из БД)
* **Инвариант**: одна таблица на инструмент, таблица называется `InstrumentConfig.name` (= `active_future_symbol`).

---

### 9) `PriceCollector`

* **Где**: `core/price_get.py`
* **Что**: загрузчик исторических данных (IB historical data → PriceDB)
* **Кто создаёт**: `robot.py` (на старте) и `services/price_streaming_runtime.py` (перед стримом для gap backfill)
* **Ключевые методы**:

  * `sync_many(instrument_configs, ...)` — массовая первичная загрузка
  * `sync_history_for(cfg, ...)` — точечная дозагрузка дыр

---

### 10) `PriceStreamer`

* **Где**: `core/price_get.py`
* **Что**: стриминг realtime баров (IB subscriptions → PriceDB)
* **Кто создаёт**: `services/price_streaming_runtime.py`
* **Ключевой метод**:

  * `stream_bars(cfg, cancel_event)` — непрерывный поток баров

---

### 11) `QuietWindowsService`

* **Где**: `services/quiet_windows.py`
* **Что**: движок правил “окон тишины” (entry/exit gating)
* **Хранилище**: `(robot_ops.db)` таблица `quiet_windows`
* **Кто создаёт**: `robot.py` (один экземпляр на процесс)
* **Кто потребляет**:

  * `services/trading_runtime.py` — перед входом/выходом
  * `services/telegram_formatters.py` — для вывода правил при старте
* **Ключевые методы**:

  * `evaluate(robot_id, now_utc, action_type="entry"/"exit") -> QuietDecision`
  * `get_enabled_rules_for_robot(...) -> list[dict]`
  * `seed_default_rth_open(robot_id)`

---

### 12) `QuietDecision`

* **Где**: возвращаемый объект из `QuietWindowsService.evaluate(...)` (в `services/quiet_windows.py`)
* **Что**: решение “разрешено/запрещено” для действия (entry/exit) в текущий момент
* **Ключевые поля (обычно)**:

  * `allowed: bool`
  * `reason: str | None`
  * `rule_id: int | None`

---

### 13) `_QuietState` (runtime notify state)

* **Где**: `services/trading_runtime.py`
* **Что**: состояние уведомлений/антиспама по quiet windows в рамках одного trading loop
* **Кто создаёт**: внутри `trading_loop()` (один на робота)
* **Кто потребляет**: только `trading_loop()`
* **Важно**: не хранится в `quiet_windows.py` и не должен быть в `robot.py`.

---

### 14) `Strategy` и `StrategySignal`

* **Где**: контракт в `contracts/bot_spec.py`, реализации в `strategies/*`
* **Что**:

  * `Strategy` — логика принятия решения (обычно читает данные из `price.db`/других источников)
  * `StrategySignal` — результат вычисления (например `"long"|"short"|"flat"`, reason, диагностика)
* **Кто создаёт**: `RobotSpec.strategy_factory()`
* **Кто потребляет**: `services/trading_runtime.py` (внутри trading loop)

---

### 15) `TradeEngine`

* **Где**: `core/trade_engine.py`
* **Что**: слой исполнения сделок (IB orders)
* **Кто создаёт**: `robot.py` (один экземпляр на процесс)
* **Кто потребляет**: `services/trading_runtime.py`
* **Ключевой метод**:

  * `market_order(contract, action, quantity, timeout, order_ref, ...) -> (order, result)`

---

### 16) Telegram: `TelegramChannel`

* **Где**: `core/telegram.py`
* **Что**: отправка сообщений в Telegram
* **Кто создаёт**: `robot.py` (2 канала: common/logs и trading)
* **Кто потребляет**:

  * `robot.py` — старт/ошибки/остановка
  * `services/trading_runtime.py` — сделки и эксплуатационные уведомления
  * мониторинги — heartbeat/портфель
* **Инвариант**: в проекте чётко разделены каналы (лог-канал vs торговый).

---