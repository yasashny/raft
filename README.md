# Raft KV — in-memory key–value хранилище на кластере Raft из 5 узлов

Реализация алгоритма консенсуса [Raft](https://raft.github.io/) с нуля на Kotlin, поверх которой
работает реплицируемое in-memory key–value хранилище. Пять независимых процессов-узлов выбирают
лидера, реплицируют журнал команд и переживают краши и сетевые разделения. Отдельный процесс CLI
управляет кластером, наблюдает за ним и инъектирует отказы.

Сторонние библиотеки консенсуса/RPC/сериализации не используются — только стандартная библиотека
Kotlin и `kotlin-test`. JSON и сетевой протокол написаны вручную (см. `json/Json.kt`).

> Объём (по заданию): без log compaction/snapshotting, фиксированный состав из 5 узлов, без
> linearizable reads (достаточно чтения с лидера), без дедупликации клиентских сессий.

---

## 1. Архитектура

Два типа процессов:

| Процесс | Роль |
|---------|------|
| **Node** (`n1`…`n5`) | Узел Raft + реплика KV. По одному процессу на каждый, свой `--data-dir/<id>/`. Общается консенсусом с остальными четырьмя; обслуживает клиентов. |
| **CLI** | Точка входа, наблюдатель и инъектор отказов. **Не участвует в консенсусе** — никогда не координатор. Также порождает процессы узлов для `startNode`. |

```
              ┌─────┐ RequestVote / AppendEntries ┌─────┐
   CLI ──────▶│ n1  │◀───────────────────────────▶│ n3  │
 (put/get,    │LEAD │◀──┐                       ┌─▶│FOLL │
  status,     └─────┘   │   ┌─────┐   ┌─────┐   │  └─────┘
  killNode,   ┌─────┐   └──▶│ n2  │   │ n4  │◀──┘  ┌─────┐
  partition)─▶│ n5  │◀─────▶│FOLL │   │FOLL │◀────▶│ ... │
              └─────┘       └─────┘   └─────┘      └─────┘
```

Каждый узел слушает один TCP-порт **и** для трафика peer'ов, **и** для клиентского/админского трафика;
сообщения различаются по полю `type`, а не по соединению. Консенсус-RPC смоделированы как
односторонние сообщения (`RequestVote` и его `RequestVoteResp` — два независимых отправления),
поэтому event-loop никогда не блокируется на сети.

### Раскладка модулей (`src/main/kotlin/com/yasashny/raft/`)

```
json/         Json.kt                      JSON-кодер/парсер вручную
model/        LogEntry, Role, Messages     доменные сущности + сетевой протокол
storage/      DurableState (интерфейс)     граница персистентности …
              FileDurableState             … на файлах (meta + append-only log)
              InMemoryDurableState         … in-memory (тесты)
net/          Transport (интерфейс)        исходящий консенсус-обмен …
              TcpTransport                 … потоки-отправители на peer, partition + delay
              RpcServer, ReplySink         входящий accept/read-цикл, канал ответа клиенту
node/         NodeConfig, RaftNode         однопоточный actor: состояние, event loop, механизмы
election/     ElectionManager              §6 Leader Election
replication/  ReplicationManager           §7 Log Replication
cli/          Cli, ClusterClient, Bench …  REPL, leader-aware клиент, бенчмарки
NodeMain.kt   CliMain.kt                   точки входа процессов
```

### Модель конкурентности

Каждый узел — **однопоточный actor**. Всем состоянием Raft владеет один поток event-loop'а
(`RaftNode.runLoop`); потоки-читатели сети и CLI никогда не меняют состояние напрямую — они кладут
задачу в очередь (`submit`), и цикл её выполняет. **Таймеры — это не потоки**: цикл делает `poll`
очереди с таймаутом, равным ближайшему дедлайну, поэтому истёкший таймер выборов/heartbeat'а — это
просто `null` от `poll`'а. Это убирает гонки данных by construction (без блокировок, без дедлоков),
при этом дисковый I/O, входящие RPC и таймеры идут конкурентно *вне* цикла. TCP-слой добавляет
поток-acceptor, по одному потоку-читателю на соединение и по одному потоку-отправителю на peer'а
(каждый со своей очередью).

---

## 2. Persistent state и инварианты log-before-reply

Хранится в `<data-dir>/<nodeId>/` (формат — в `FileDurableState`):

- **`meta`** — один JSON-объект `{"currentTerm":int,"votedFor":string|null}`, перезаписывается
  атомарно (`meta.tmp` → `force` → `ATOMIC_MOVE`), чтобы краш не мог оставить его наполовину
  записанным.
- **`log`** — append-only **JSON Lines**, по одному `{"term":int,"command":string}` на строку. Храним
  байтовый offset каждой строки, поэтому усечение конфликта — это один `FileChannel.truncate` до
  границы.

`kv`, `commitIndex`, `lastApplied` **никогда** не персистятся — они восстанавливаются повторным
проигрыванием (replay) committed-префикса (`commitIndex`/`lastApplied` сбрасываются в 0 при рестарте и
догоняются из `leaderCommit` лидера).

**Соблюдаемые инварианты (проверяемы через `tail -f data/n*/log` во время прогона):**

1. `currentTerm`/`votedFor` попадают на диск **до** любого ответа, меняющего term, и до
   `RequestVoteResp`, отдающего голос — иначе рестарт мог бы привести к двойному голосованию → два
   лидера. (`stepDownIfHigher`, `ElectionManager.onRequestVote` вызывают `persistMeta()` до `send`.)
2. Follower записывает `suffix` (и усечение) на диск **до** ответа `success=true`.
   (`ReplicationManager.appendEntriesLocal` → `node.appendToLog`/`truncateLogTo` до `send`.)
3. Лидер записывает новый entry на диск **до** рассылки AppendEntries.
   (`ReplicationManager.onClientPut` → `appendToLog` до `replicateToAll`.)

**Режим fsync** (`--fsync on|off`, фиксирован на весь прогон процесса):

- `on` (по умолчанию): каждая дозапись журнала и каждое обновление `meta` сбрасываются через
  `FileChannel.force(true)` **до** соответствующего сетевого действия — инварианты выше держатся даже
  против потери питания.
- `off`: байты пишутся, но не форсятся; `killNode` (краш процесса) данные сохранит (они в страничном
  кэше ОС), но реальная потеря питания может потерять несинхронизированный хвост и нарушить инварианты.
  Используется только как «потолок без durability» в части C бенчмарка. При восстановлении порванная
  последняя строка журнала обнаруживается и отбрасывается.

---

## 3. Сетевой протокол

TCP, **JSON Lines** (один JSON-объект на строку, завершается `\n`). Один порт на узел для всего.

**Консенсус** (узел ↔ узел):

```json
{"type":"REQUEST_VOTE","term":3,"candidateId":"n4","lastLogIndex":47,"lastLogTerm":2}
{"type":"REQUEST_VOTE_RESP","nodeId":"n2","term":3,"voteGranted":true}
{"type":"APPEND_ENTRIES","term":3,"leaderId":"n1","prefixLen":4,"prefixTerm":3,"leaderCommit":4,"suffix":[{"term":3,"command":"put x 8"}]}
{"type":"APPEND_ENTRIES_RESP","nodeId":"n2","term":3,"ack":5,"success":true}
```

Соответствие именам из оригинальной статьи: `prefixLen ↔ prevLogIndex`, `prefixTerm ↔ prevLogTerm`,
`suffix ↔ entries[]`, `leaderCommit ↔ leaderCommit`. Все индексы/длины — это **длины** (количества), а
не 0-базовые индексы: `commitIndex = 3` значит, что закоммичены `log[0..2]`.

**Клиент / админ** (CLI → узел): `CLIENT_PUT`, `CLIENT_GET`, `LEADER_QUERY`, `STATUS_QUERY`,
`LOG_DUMP`, `KILL`, `SET_PARTITION`, `HEAL`, `SET_DELAY`, `SET_ELECTION_TIMEOUT`. У каждого клиентского
запроса есть `requestId`+`type`; ответы несут `requestId`, `status` (`OK|ERROR`), и при `OK` — `value`
(GET) или `committedIndex` (PUT).

**Ошибки** (в `ClientResponse.error`): `NOT_LEADER` (с `leaderHint`), `NO_LEADER`, `TIMEOUT`,
`BAD_REQUEST`, `UNKNOWN_NODE`.

### Семантика `get` — **чтение с лидера**

По умолчанию `get` обслуживается **только лидером**, из его применённой `kv`-карты (без лишнего
round-trip'а). Follower отвечает `NOT_LEADER` + `leaderHint`. Это принятый заданием trade-off: не
linearizable, и сразу после смены лидера чтение может быть кратко устаревшим, пока новый лидер не
закоммитит свой первый entry текущего term'а (оговорка про term) — что инициирует любой последующий
`put`. Устаревшие чтения с follower'а намеренно *не* предлагаются.

---

## 4. Сборка и запуск

Нужен JDK (24+). Собрать fat-jar (включает stdlib Kotlin):

```bash
./gradlew clean jar      # → build/libs/raft.jar
./gradlew test           # 23 юнит-теста (safety выборов, репликация, оговорка про term, persistence)
```

### Проще всего: CLI сам поднимает кластер

```bash
java -jar build/libs/raft.jar
raft> startCluster --clean      # поднимает n1..n5 (127.0.0.1:9001..9005), data dir ./data
raft> put x 1
raft> get x                     # → 1
raft> status
raft> leader
raft> help                      # полный список команд
```

`startNode`/`killNode` — это реальный жизненный цикл процесса: `killNode` делает `System.exit` узла
(состояние остаётся на диске); `startNode` заново порождает процесс с тем же `--data-dir`, и узел
восстанавливается из журнала. CLI запускает узлы через `ProcessBuilder`, переиспользуя свой classpath
(`java -cp build/libs/raft.jar com.yasashny.raft.NodeMainKt …`); stdout/stderr узлов идут в
`data/<id>.log`.

### Вручную: запустить узел руками

```bash
java -cp build/libs/raft.jar com.yasashny.raft.NodeMainKt \
  --id n1 --port 9001 --data-dir ./data --fsync on \
  --peers n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003,n4=127.0.0.1:9004,n5=127.0.0.1:9005 \
  --election-min 150 --election-max 300 --heartbeat 50
```

Флаги запуска: `--id`, `--port`, `--data-dir` (корень; узел использует `<root>/<id>/`),
`--fsync on|off`, `--peers` (полная таблица, self включается и игнорируется), опционально
`--election-min/--election-max/--heartbeat`, `--verbose on|off`.

### Наблюдаемость (логи)

Каждый узел пишет структурированный лог в `data/<id>.log`, каждая строка начинается с его текущего
состояния (`term`, `role`, `commit`, `logLen`), затем — событие. При `--verbose on` (по умолчанию)
логи показывают весь трафик консенсуса:

```
term=1 CANDIDATE | start election → RequestVote(term=1, lastLogIdx=0, lastLogTerm=0) to [n2,n3,n4,n5]
term=1 FOLLOWER  | ← RequestVote from n1 (term=1, lastLog=0/0) → GRANT (votedFor persisted)
term=1 CANDIDATE | ← vote from n2: GRANTED (tally=3/3)
term=1 LEADER    | ← CLIENT_PUT 'put x 1' → log[1] (term 1), persisted; replicating
term=1 LEADER    | → AppendEntries n2: prefixLen=0 prefixTerm=0 entries=1 leaderCommit=0
term=1 FOLLOWER  | ← AppendEntries from n1: prefixLen=0 entries=1 leaderCommit=0 → ACCEPT, persisted, ack=1
term=1 LEADER    | commit advanced to 1 (majority ack, term caveat satisfied)
term=3 LEADER    | ← ack from n5: FAIL → backtrack sentLength=4, resend
term=2 FOLLOWER  | TRUNCATE log 2 → 1 (conflicting uncommitted tail dropped)
term=2 FOLLOWER  | steps down: observed higher term 2
```

Так что видно, как растут term'ы, меняются роли, каждый RequestVote/AppendEntries (отправленный и
полученный), продвижение `commitIndex`, backtracking и усечение конфликтов. Heartbeat'ы в
установившемся режиме троттлятся до ~1/с (`♥ …`), чтобы не топить лог. `--verbose off` оставляет только
редкие события (выборы, смены ролей, усечения, троттленные heartbeat'ы) — бенчмарк-сьют использует его,
чтобы пер-RPC логирование не искажало throughput.

### Демо и бенчмарки

- **`demo-scenario.md`** — копируемые команды для 5 обязательных сценариев.
- **`benchmarks/`** — `benchSuite` прогоняет весь обязательный сьют в `benchmarks/results.csv`;
  `python3 benchmarks/plot.py` рисует 3 графика; анализ в **`report.md`**.

---

## 5. Соответствие псевдокоду семинаров

| Семинар | Где |
|---------|-----|
| §5 универсальное правило term | `RaftNode.stepDownIfHigher` |
| §6.2 процедура выборов | `ElectionManager.startElection` |
| §6.3 отдача голоса + up-to-date check | `ElectionManager.onRequestVote` |
| §6.4 победа / split-vote | `ElectionManager.onRequestVoteResponse` / `becomeLeader` |
| §7.2 ReplicateLog | `ReplicationManager.replicateLog` |
| §7.3 AppendEntries + `logOk` | `ReplicationManager.onAppendEntries` |
| §7.4 усечение / дозапись / deliver | `ReplicationManager.appendEntriesLocal` |
| §7.5 ответ + backtrack | `ReplicationManager.onAppendEntriesResponse` |
| §7.6 CommitLogEntries + **оговорка про term** | `ReplicationManager.commitLogEntries` |
