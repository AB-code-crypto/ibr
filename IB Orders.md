У IBKR “типы ордеров” лучше мыслить в двух плоскостях:

1. **Базовый `orderType` (строковый код в API)** — то, что вы задаёте в `Order.orderType` и связанных полях. Полный перечень доступных через API типов IBKR документирует на отдельной странице “Order Types | IBKR API” (с минимальными обязательными полями). ([Interactive Brokers][1])
2. **Комбинации/механики** (Bracket, OCA/OCO, Conditional, Algos) — это не всегда отдельный `orderType`, часто это связка нескольких ордеров + атрибуты. ([interactivebrokers.github.io][2])

IB прямо заявляет, что предоставляет **100+** вариантов (включая “order types, algos and tools”), а ядро — Market/Limit/Stop/Stop-Limit/Market-to-Limit. ([Interactive Brokers][3])

Ниже — практичная классификация, полезная для вашего `ib_order_api.py`.

---

## 1) Базовые (ядро)

* **Market (`MKT`)** — исполнение по рынку. ([interactivebrokers.github.io][4])
* **Limit (`LMT`)** — по заданной цене или лучше.
* **Stop (`STP`)** — триггер по stopPrice, после чего обычно отправляется market.
* **Stop-Limit (`STPLMT`)** — триггер по stopPrice, после чего выставляется limit. ([Interactive Brokers U.K. Limited][5])
* **Market-to-Limit (`MTL`)** — стартует как market, остаток становится limit. ([Interactive Brokers][3])

Эти 5 покрывают большую часть реальной торговли.

---

## 2) “Touched” ордера (срабатывают при касании уровня)

* **MIT (Market If Touched)** — при касании уровня отправляет market.
* **LIT (Limit If Touched)** — при касании уровня отправляет limit.

---

## 3) Trailing-семейство (трейлинг)

В IB это несколько отдельных `orderType` кодов, наиболее часто встречаются:

* **TRAIL** — trailing stop (стоп “едет” за ценой).
* **TRAILLMT** — trailing stop-limit.
* **TRAILMIT / TRAILLIT** — trailing market/limit if touched (варианты “touched” с трейлингом). ([IBKR Guides][6])

Для вас важно: в API это реально разные `orderType` строки (а не один “trailing”). Поэтому в адаптере удобно держать и `TRAIL`, и `TRAILLMT`, а остальное давать через универсальный `order_kwargs`.

---

## 4) Pegged / Relative / “привязанные” к котировкам

Типы, которые “едут” за рынком, но по правилам, а не как trailing stop:

* **REL / REL2MID / RELSTK** и т.п. (relative-семейство) ([IBKR Guides][6])
* **Pegged to midpoint / Snap to …** (семейство “SNAP*”, midpoint-механики) — зависит от инструмента/площадки. ([IBKR Guides][6])

---

## 5) На открытии/закрытии, аукционы (где применимо)

* **MOO / LOO** — market/limit on open
* **MOC / LOC** — market/limit on close
  Это сильно зависит от рынка/инструмента и доступности в конкретной связке “security type + exchange”.

---

## 6) Алгос IBKR (важно: часто это не новый `orderType`, а параметры algo)

IBKR поддерживает “Advanced order types / IB Algos” (TWAP/VWAP/Adaptive и др.) — они обычно задаются через поля algo-параметров у ордера, а не только `orderType`. IB прямо отмечает, что “любой тип ордера, который есть в TWS, может быть размещён через API”, включая advanced/algos/conditional. ([Interactive Brokers][7])

---

## 7) Комбинации и механики, которые вам уже нужны для робота

Это то, что вы как раз реализуете “связками”, а не одним `orderType`:

* **Bracket** — parent + два child (TP limit и SL stop), один срабатывает — второй отменяется. ([interactivebrokers.github.io][2])
* **OCA (One-Cancels-All)** — группа ордеров, где при начале исполнения одного остальные отменяются (это и есть практическая реализация OCO на стороне IB). ([Interactive Brokers][8])
* **Conditional orders** — отправка/активация ордера при выполнении условий (цена, время, индексы и т.д.). ([Interactive Brokers][9])

---

## 8) Практическое замечание для вашего `ib_order_api.py`

Если ваша цель — “поддержать **все** типы IB без раздувания кода”, то самая сильная конструкция такая:

* иметь несколько **билдеров для самых частых** (MKT/LMT/STP/STPLMT/TRAIL/TRAILLMT/MIT/LIT/MTL)
* и обязательно держать **универсальный путь**: “передай готовый `Order` с любым `orderType` и любыми полями — и я просто отправлю”
  Потому что полный список реально большой (100+), и он варьируется по инструментам и площадкам. ([Interactive Brokers][3])

список наиболее полезных `orderType` кодов + какие поля обязательны (lmtPrice/auxPrice/trailingPercent и т.д.) в формате таблицы, чтобы вы сразу закрыли тест-меню внизу `ib_order_api.py`.

[1]: https://www.interactivebrokers.com/campus/ibkr-api-page/order-types/?utm_source=chatgpt.com "Order Types | IBKR API | IBKR Campus"
[2]: https://interactivebrokers.github.io/tws-api/bracket_order.html?utm_source=chatgpt.com "TWS API v9.72+: Bracket Orders"
[3]: https://www.interactivebrokers.com/campus/glossary-terms/order-types-and-algos/?utm_source=chatgpt.com "Order Types and Algos - IBKR Campus"
[4]: https://interactivebrokers.github.io/tws-api/basic_orders.html?utm_source=chatgpt.com "TWS API v9.72+: Basic Orders"
[5]: https://www.interactivebrokers.co.uk/en/software/tws.bak/usersguidebook/ordertypes/stop_limit.htm?utm_source=chatgpt.com "Stop Limit"
[6]: https://www.ibkrguides.com/reportingreference/reportguide/order-types.htm?utm_source=chatgpt.com "Order Types"
[7]: https://www.interactivebrokers.com/campus/trading-lessons/python-placing-orders/?utm_source=chatgpt.com "Placing Orders using TWS Python API | Trading Lesson"
[8]: https://www.interactivebrokers.com/campus/trading-lessons/tws-mosaic-one-cancels-all-oca-order-type/?utm_source=chatgpt.com "TWS Mosaic One-Cancels-All (OCA) Order Type"
[9]: https://www.interactivebrokers.com/campus/trading-lessons/conditional-orders/?utm_source=chatgpt.com "Conditional Orders | Trading Lesson | Traders' Academy"


Ниже — “шпаргалка” по `orderType` (коды IB/TWS API) и **какие поля реально нужны**, чтобы ордер был валидным. Сразу оговорка: у IBKR **десятки/сотни вариантов** (включая алго/условные/пеггед и т.д.), но 90% практики закрывается ограниченным набором, а остальное вы всегда можете отправлять через “универсальный” `Order()` с нужными полями. ([ibkrguides.com][1])

## Общие поля (почти для любого ордера)

Минимально везде:

* `action`: `"BUY"` / `"SELL"`
* `totalQuantity`
* `orderType`

Дальше — **price-поля и спецатрибуты** по типу ордера.

---

## Time-in-Force и TTL (время жизни)

### Базовые TIF, которые чаще всего нужны

* `DAY`, `GTC`
* `IOC`, `FOK`
* `OPG` (используется для MOO/LOO) ([Interactive Brokers U.K. Limited][2])
* `GTD` (good-till-date) — **для TTL/истечения по времени** ([Interactive Brokers U.K. Limited][2])

### Как задавать TTL в API (2 способа)

1. `tif="GTD"` + `goodTillDate` (дата-время, когда ордер должен отмениться) — поле **валидно только при GTD**. ([interactivebrokers.com][3])
2. `tif="GTD"` + `duration` (секунды жизни) — IB прямо отмечает, что `Duration` и `GoodTillDate` **нельзя указывать одновременно**. ([interactivebrokers.com][3])

Формат GTD/времени в интерфейсе/гайде: дата `YYYYMMDD`, время `HH:MM(:SS)` (при желании — таймзона). ([Interactive Brokers U.K. Limited][2])

---

## Таблица: частые `orderType` и обязательные поля

| `orderType` (код)   | Что это                | Какие поля цены обязательны                                                   | Примечания                                                                                                                                                          |
| ------------------- | ---------------------- | ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `MKT`               | Market                 | —                                                                             | Только `action`, `totalQuantity`. ([interactivebrokers.github.io][4])                                                                                               |
| `LMT`               | Limit                  | `lmtPrice`                                                                    | Классический лимитник. ([interactivebrokers.github.io][4])                                                                                                          |
| `STP`               | Stop (market stop)     | `auxPrice` (= stopPrice)                                                      | Стоп-триггер в `auxPrice`. ([interactivebrokers.github.io][4])                                                                                                      |
| `STP LMT`           | Stop-Limit             | `auxPrice` (= stopPrice) + `lmtPrice`                                         | В документации код с пробелом: `"STP LMT"`. ([interactivebrokers.github.io][4])                                                                                     |
| `MIT`               | Market-If-Touched      | `auxPrice` (= trigger)                                                        | При касании уровня отправляет market. ([interactivebrokers.github.io][4])                                                                                           |
| `LIT`               | Limit-If-Touched       | `auxPrice` (= trigger) + `lmtPrice`                                           | При касании уровня активируется лимитник. ([interactivebrokers.github.io][4])                                                                                       |
| `MTL`               | Market-to-Limit        | —                                                                             | Исполняет как market, остаток превращает в limit по цене исполнения. ([interactivebrokers.github.io][4])                                                            |
| `MOC`               | Market-on-Close        | —                                                                             | Рыночный на закрытии. ([interactivebrokers.github.io][4])                                                                                                           |
| `LOC`               | Limit-on-Close         | `lmtPrice`                                                                    | Лимитный на закрытии. ([interactivebrokers.github.io][4])                                                                                                           |
| `MKT` + `tif="OPG"` | Market-on-Open (MOO)   | —                                                                             | В API это `orderType="MKT"` + `tif="OPG"`. ([interactivebrokers.github.io][4])                                                                                      |
| `LMT` + `tif="OPG"` | Limit-on-Open (LOO)    | `lmtPrice`                                                                    | В API это `orderType="LMT"` + `tif="OPG"`. ([interactivebrokers.github.io][4])                                                                                      |
| `TRAIL`             | Trailing Stop          | `trailStopPrice` + (`trailingPercent` **или** `auxPrice` как trailing amount) | В примере: `trailingPercent` + `trailStopPrice`. ([interactivebrokers.github.io][4])                                                                                |
| `TRAIL LIMIT`       | Trailing Stop Limit    | `trailStopPrice` + `lmtPriceOffset` + `auxPrice` (trailingAmount)             | Важный нюанс: в TRAIL LIMIT нельзя одновременно задавать “limit price” и “limit offset”; в примере используют `lmtPriceOffset`. ([interactivebrokers.github.io][4]) |
| `MKT PRT`           | Market with Protection | —                                                                             | Полезно на некоторых фьючерсных маршрутах (например Globex). ([interactivebrokers.github.io][4])                                                                    |
| `STP PRT`           | Stop with Protection   | `auxPrice` (= stopPrice)                                                      | Триггер-стоп, далее “market with protection”. ([interactivebrokers.github.io][4])                                                                                   |
| `PEG MKT`           | Pegged-to-Market       | `auxPrice` (= offset)                                                         | Пеггед к bid/ask с оффсетом. ([interactivebrokers.github.io][4])                                                                                                    |
| `PEG MID`           | Pegged-to-Midpoint     | `auxPrice` (= offset) + `lmtPrice`                                            | В примере задан и offset, и “worst limit”. ([interactivebrokers.github.io][4])                                                                                      |

Этого набора достаточно, чтобы руками обкатать: лимитники, стопы, touched, трейлинги, “на открытии/закрытии”, TTL, и несколько “полезных спецтипов”.

---

## OCO / OCA (когда один исполнился — второй отменился)

В IB это делается через **OCA group**:

* всем ордерам в группе ставите одинаковый `ocaGroup`
* задаёте `ocaType` (как именно обрабатывать остальные ордера при исполнении одного) ([interactivebrokers.github.io][5])

`ocaType` по справочнику:

* `1` — отменить оставшиеся (block)
* `2` — пропорционально уменьшить размеры (block)
* `3` — пропорционально уменьшить размеры (no block) ([interactivebrokers.github.io][6])

Для классического “OCO” обычно используют `ocaType=1`.

---

[1]: https://www.ibkrguides.com/traderworkstation/order-types.htm?utm_source=chatgpt.com "Order Types"
[2]: https://www.interactivebrokers.co.uk/en/software/tws.bak/usersguidebook/ordertypes/time_in_force_for_orders.htm "Time in Force for Orders"
[3]: https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-ref/ "TWS API Reference | IBKR API | IBKR Campus"
[4]: https://interactivebrokers.github.io/tws-api/basic_orders.html "TWS API v9.72+: Basic Orders"
[5]: https://interactivebrokers.github.io/tws-api/oca.html "TWS API v9.72+: One Cancels All"
[6]: https://interactivebrokers.github.io/tws-api/classIBApi_1_1Order.html?utm_source=chatgpt.com "TWS API v9.72+: Order Class Reference"


В IBKR под “типами исполнения” обычно имеют в виду две вещи:

1. **Режим/условия исполнения ордера** (как именно биржа/маршрутизатор должен пытаться заполнить объём: сразу/целиком/частями, сколько времени держать, показывать ли объём в стакане и т.д.).
2. **События/результат исполнения** (частичное/полное исполнение, отмена, отклонение — что вы увидите в `orderStatus`/`fills`).

Ниже — оба блока, в терминах IB API.

## 1) Условия исполнения (execution instructions / ограничения)

### A) “Сразу или отменить” / “целиком или отменить”

Это то, что чаще всего называют именно “типом исполнения”:

* **IOC (Immediate-Or-Cancel)** — исполнить сразу доступную часть, **остаток отменить**. ([interactivebrokers.github.io][1])
  *Поле:* `order.tif = "IOC"`.

* **FOK (Fill-Or-Kill)** — если **весь объём** не может быть исполнен сразу, **весь ордер отменяется**. ([interactivebrokers.github.io][1])
  *Поле:* `order.tif = "FOK"`.

* **AON (All-Or-None)** — ордер не должен исполняться частями: либо целиком, либо никак (может “висеть”, пока не появится весь объём). ([Interactive Brokers U.K. Limited][2])
  *Поле:* `order.allOrNone = True`.

* **MinQty (минимально приемлемое исполнение)** — разрешить частичные исполнения, но **не меньше заданного минимума** за одно исполнение/активацию. (В IB это поле есть как атрибут ордера.) ([CRAN][3])
  *Поле:* `order.minQty = N`.

### B) Время жизни (Time-in-Force) и “когда ордер активен”

* **DAY** — действует до конца торгового дня.

* **GTC** — действует до отмены.

* **GTD** — действует до указанной даты/времени (это ваш **TTL**). ([interactivebrokers.github.io][1])
  *Поля:* `order.tif="GTD"` + `order.goodTillDate="yyyyMMdd HH:mm:ss (TZ)"` или UTC-формат. ([interactivebrokers.github.io][1])

* **OPG** — режим “на открытие” (для MOO/LOO). ([interactivebrokers.github.io][1])

* **DTC** — “day until canceled” (встречается в справочнике IB API как отдельный TIF). ([interactivebrokers.github.io][1])

### C) Как исполнять “в стакане” (видимость/скорость)

* **Iceberg/Reserve (DisplaySize)** — показывать в стакане только часть объёма, остальное скрыто; IB использует `DisplaySize` для iceberg. ([interactivebrokers.github.io][1])
  *Поле:* `order.displaySize = ...`.

* **Sweep-to-Fill** — агрессивно “сметать” ликвидность по уровням ради скорости (в ущерб цене). ([interactivebrokers.github.io][4])
  *Поле:* `order.sweepToFill = True`.

* **Hidden** — скрытый ордер (есть ограничения по маршрутам/рынкам). ([interactivebrokers.github.io][1])
  *Поле:* `order.hidden = True`.

### D) Исполнение вне основной сессии

* **Outside RTH** — разрешить триггеры/исполнение вне регулярных торговых часов (где применимо). ([interactivebrokers.github.io][1])
  *Поле:* `order.outsideRth = True`.

### E) “Когда активировать”

* **GoodAfterTime** — ордер активен только после указанного времени. ([interactivebrokers.github.io][1])
  *Поле:* `order.goodAfterTime = "yyyymmdd hh:mm:ss {TZ}"`.

### F) Для simulated stop/trailing — “как триггерить”

* **TriggerMethod** — чем триггерить simulated Stop/Stop-Limit/Trailing (last vs double bid/ask и т.п.). ([interactivebrokers.github.io][1])
  *Поле:* `order.triggerMethod = ...`.

## 2) Результат исполнения (что вы увидите от брокера)

В IB это приходит как комбинация:

* **orderStatus** (жизненный цикл ордера)
* **executions/fills** (факты исполнения, может быть несколько “fills” на один ордер)

### Типовые статусы, которые важно различать

* **PendingSubmit** — вы отправили, но ещё нет подтверждения, что ордер принят местом назначения. ([Interactive Brokers Hong Kong Limited][5])
* **Submitted** — ордер принят и “работает”. ([Interactive Brokers Hong Kong Limited][5])
* **Filled** — полностью исполнен. ([Interactive Brokers Hong Kong Limited][5])
* Также на практике важны: **Cancelled**, **Inactive**, **Rejected** (часть из них перечисляется в справочниках статусов/обсуждениях API; их обязательно надо обрабатывать как терминальные). ([groups.io][6])

### Исполнение может быть:

* **Полное (fill)** — одна или несколько сделок, но итоговый remaining=0.
* **Частичное (partial fills)** — несколько “fills” (исполнений) на один ордер; это нормальная ситуация, особенно на лимитниках/больших объёмах. AON/MinQty как раз про то, чтобы это ограничивать. ([Interactive Brokers][7])

---

[1]: https://interactivebrokers.github.io/tws-api/classIBApi_1_1Order.html?utm_source=chatgpt.com "TWS API v9.72+: Order Class Reference"
[2]: https://www.interactivebrokers.co.uk/en/software/tws.bak/usersguidebook/ordertypes/all_or_none__aon.htm?utm_source=chatgpt.com "All or None (AON)"
[3]: https://cran.r-project.org/web/packages/IBrokers/IBrokers.pdf?utm_source=chatgpt.com "IBrokers: R API to Interactive Brokers Trader Workstation"
[4]: https://interactivebrokers.github.io/tws-api/basic_orders.html?utm_source=chatgpt.com "TWS API v9.72+: Basic Orders"
[5]: https://www.interactivebrokers.com.hk/en/software/mobileTrader/pda/stock_8description.htm?utm_source=chatgpt.com "Order Management"
[6]: https://groups.io/g/twsapi/topic/table_of_possible_status/12124023?utm_source=chatgpt.com "Table of possible \"status\" strings from orderStatus()"
[7]: https://www.interactivebrokers.com/campus/trading-lessons/all-or-none-aon/?utm_source=chatgpt.com "All or None (AON) | Trading Lesson | Traders' Academy"
[8]: https://www.interactivebrokers.com/campus/glossary-terms/iceberg-reserve-order/?utm_source=chatgpt.com "Iceberg/Reserve Order - IBKR Campus"
