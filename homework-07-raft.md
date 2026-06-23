# Домашнее задание №7: Raft — консенсус от теории к практике

**Максимальный балл:** 10 (8 — функциональность, 2 — качество кода)
**Язык:** Java/Kotlin или Go
**Формат:** Pull Request в **новый** репозиторий + защита

---

## Цель работы

В ДЗ №2–№5 порядок записей задавал заранее назначенный лидер: кто лидер — решал человек через CLI, а не сам кластер.
В ДЗ №6 атомарность обеспечивал координатор, но 2PC блокируется, если координатор упал в `PREPARED`-окне, — у протокола нет способа самостоятельно выбрать новый «мозг».
Оба раза недоставало одного: автоматического, доказуемо безопасного выбора лидера и согласованного журнала команд, переживающего падения.

Эту задачу решает **консенсус**. На семинарах мы прошли путь от классического Paxos к Raft.
Paxos описывает идею (две фазы, пересечение кворумов), но не специфицирует, как именно выбирать лидера, как физически устроен журнал,
что делать при сетевом разделении.

Поэтому каждая реализация «Paxos» (Chubby, Spanner, Cassandra LWT) под капотом была своей, а одинаковые баги всплывали везде.
Raft — это та же идея Multi-Paxos, но **специфицированная до конца**: явная роль Candidate, term как эпоха лидерства, log matching, up-to-date check при голосовании.
Именно полнота спецификации сделала Raft стандартом (etcd, CockroachDB, Consul, TiKV).

В этой работе нужно реализовать **in-memory key-value хранилище поверх Raft-кластера из 5 узлов**.
Клиент отправляет команды `put key value` и `get key`; каждая запись проходит через реплицируемый журнал Raft. В результате должны получиться:

1. Узлы с тремя ролями (Follower / Candidate / Leader) и корректными переходами по правилам Raft.
2. **Leader Election** с randomized timeout и up-to-date check.
3. **Log Replication** строго по псевдокоду семинаров 30–32: `logOk`, backtrack, commit с оговоркой про term.
4. **Persistent state** (`currentTerm`, `votedFor`, `log[]`) на диске, переживающий рестарт процесса.
5. CLI для наблюдаемости и инъекции отказов (`killNode`, `partition`, `setDelay`) + бенчмарки.

---

## 1. Что требуется и что не требуется

**Требуется:**
- **5 узлов**, каждый — отдельный процесс с собственным `--data-dir`;
- TCP-соединения, JSON Lines, отдельный процесс CLI;
- полный цикл выборов: RequestVote, randomized election timeout, heartbeat;
- полный цикл репликации: AppendEntries, `logOk`, усечение, backtrack, commit;
- persistent state на диске и корректное восстановление после `kill` + рестарта;
- инструменты инъекции отказов: `killNode`, `startNode`, `partition`, `healPartition`, `setDelay`;
- 5 обязательных демо-сценариев;
- бенчмарки (минимум 7 прогонов) и `report.md`.

**Не требуется:**
- log compaction / snapshotting — журнал может расти неограниченно;
- membership changes — состав кластера фиксирован: 5 узлов;
- linearizable reads — достаточно читать с лидера без дополнительного round-trip;
- client session semantics / дедупликация клиентских запросов при retry;


## Важное замечание: запрещено подтягивать в проект любые библотеки с готовыми реализациями RAFT-подобных механик.

---

## 2. Архитектура

В системе два типа процессов:

1. **Node** — узел Raft и одновременно реплика KV-хранилища. Запускается 5 экземпляров (`n1`…`n5`).
Каждый держит свой persistent state в `<data-dir>/<nodeId>/`.
Параметры запуска: --id, --port, --data-dir, --fsync on|off (по умолчанию on), адреса остальных четырёх узлов (peer-таблица фиксирована и одинакова у всех).
2. **CLI** — отдельный процесс. Отправляет клиентские команды, опрашивает состояние всех узлов, инъектирует отказы, запускает бенчмарки.

Узлы общаются только друг с другом (RequestVote, AppendEntries) и принимают команды CLI.
CLI не участвует в консенсусе и **не является координатором** — он лишь точка входа и наблюдатель.

`startNode`/`killNode` подразумевают реальный жизненный цикл процесса: `killNode` приводит к `System.exit` узла (persistent state остаётся на диске), `startNode` заново запускает процесс с тем же `--data-dir`, и узел восстанавливает состояние из журнала. Для этого CLI должен уметь порождать процесс узла (например, через `ProcessBuilder`) либо использовать тонкий launcher-скрипт.

```
data/
  n1/   currentTerm, votedFor, log
  n2/   ...
  n3/   ...
  n4/   ...
  n5/   ...
```

---

## 3. Модель данных

### 3.1. Persistent state (на диске, у каждого узла)
```text
currentTerm : int          // текущий term, монотонно растёт
votedFor    : String|null  // за кого голосовал в currentTerm
log[]       : LogEntry[]    // журнал команд
```
```text
LogEntry { term: int, command: String }   // command: "put <key> <value>"
```

### 3.2. Volatile state (в памяти, восстанавливается из лога)
```text
commitIndex : int   // сколько записей зафиксировано (длина committed-префикса)
lastApplied : int   // сколько записей применено к KV state machine
role        : { FOLLOWER, CANDIDATE, LEADER }
```
**Только у лидера** (volatile, сбрасывается при потере лидерства):
```text
sentLength[peer]  : int   // сколько записей лидер считает отправленными peer'у
ackedLength[peer] : int   // сколько записей peer реально подтвердил
```

### 3.3. KV state machine
```text
kv : Map<String, String>
```
`kv` — это **производная** от committed-префикса журнала: применяя `log[lastApplied .. commitIndex-1]` по порядку, узел детерминированно получает `kv`.
Отдельно `kv` на диск не пишется; персистентен только журнал.

> Все индексы и `commitIndex`/`lastApplied` — это **длины** (количество записей), а не 0-базовые индексы. `commitIndex = 3` означает, что зафиксированы записи `log[0]`, `log[1]`, `log[2]`.

---

## 4. Сетевой протокол

### 4.1. Транспорт и формат
- TCP-сокеты; допустимо принимать всё на одном порту и различать по полю `type`.
- **JSON Lines**: 1 JSON на строку, завершается `\n`.

### 4.2. Сообщения консенсуса

**RequestVote (кандидат → всем):**
```json
{"type":"REQUEST_VOTE","term":3,"candidateId":"n4","lastLogIndex":47,"lastLogTerm":2}
```
**RequestVoteResponse (узел → кандидату):**
```json
{"type":"REQUEST_VOTE_RESP","nodeId":"n2","term":3,"voteGranted":true}
```
**AppendEntries (лидер → follower'у); пустой `suffix` = heartbeat:**
```json
{"type":"APPEND_ENTRIES","term":3,"leaderId":"n1","prefixLen":4,"prefixTerm":3,
 "leaderCommit":4,"suffix":[{"term":3,"command":"put x 8"}]}
```
**AppendEntriesResponse (follower → лидеру):**
```json
{"type":"APPEND_ENTRIES_RESP","nodeId":"n2","term":3,"ack":5,"success":true}
```

Соответствие именам из оригинальной статьи Raft: `prefixLen ↔ prevLogIndex`, `prefixTerm ↔ prevLogTerm`, `suffix ↔ entries[]`, `leaderCommit ↔ leaderCommit`.
В псевдокоде ниже используются названия из семинаров.

### 4.3. Клиентские и админские сообщения (CLI ↔ Node)
`CLIENT_PUT`, `CLIENT_GET`, `LEADER_QUERY`, `STATUS_QUERY`, `LOG_DUMP`, `KILL`, `SET_PARTITION`, `HEAL`, `SET_DELAY`.

У клиентского запроса обязательны `requestId`, `type`. Ответ содержит `requestId`, `status` (`OK|ERROR`), при `OK` — `value` (для GET) или подтверждение коммита (для PUT).

### 4.4. Ошибки
`NOT_LEADER` (узел не лидер; в ответе обязателен `leaderHint` — известный узлу лидер или `null`), `NO_LEADER` (лидер ещё не избран), `TIMEOUT`, `BAD_REQUEST`, `UNKNOWN_NODE`.

---

## 5. Роли и переходы (state machine узла)

Каждый узел стартует как **Follower**. Term — «часы» Raft: каждое сообщение несёт term отправителя.

**Универсальное правило (проверяется первым в любом RPC):**
```text
если входящий term > currentTerm:
    currentTerm := входящий term
    votedFor    := null
    role        := FOLLOWER
    (сохранить currentTerm, votedFor на диск)
если входящий term < currentTerm: игнорировать / ответить отказом с currentTerm
```

| Из        | Событие                                        | В                      |
|-----------|------------------------------------------------|------------------------|
| FOLLOWER  | election timeout (нет heartbeat/AppendEntries) | CANDIDATE              |
| CANDIDATE | собрал большинство голосов (≥3 из 5)           | LEADER                 |
| CANDIDATE | получил AppendEntries с `term ≥ currentTerm`   | FOLLOWER               |
| CANDIDATE | election timeout (split vote)                  | CANDIDATE (новый term) |
| LEADER    | увидел `term > currentTerm`                    | FOLLOWER               |
| любая     | увидел `term > currentTerm`                    | FOLLOWER               |

В одном term'е может быть **максимум один лидер**: каждый узел голосует не более одного раза за term (через `votedFor`), а любые два большинства из 5 пересекаются.

---

## 6. Leader Election

### 6.1. Таймеры
- **election timeout** — случайный из диапазона **150–300 мс** (по умолчанию), индивидуальный у каждого узла, сбрасывается при получении валидного AppendEntries/heartbeat. Диапазон настраивается через CLI (для бенчмарка части B).
- **heartbeat interval** — **50 мс**: лидер шлёт пустой AppendEntries всем follower'ам, чтобы держать их таймеры сброшенными.

Случайность timeout'а — намеренный дизайн: она разводит истечения во времени и делает split vote редким.

### 6.2. Процедура выборов
```text
Узел F переходит в CANDIDATE:
  1. currentTerm += 1                 (на диск)
  2. votedFor := F                    (на диск)
  3. votesGranted := {F}
  4. сброс election timeout (новый random)
  5. рассылка RequestVote(term=currentTerm, candidateId=F,
                          lastLogIndex=log.length, lastLogTerm=term последней записи (или 0))
```

### 6.3. Условие голосования (на получателе RequestVote)
Узел X отдаёт голос за кандидата C, если **все** условия выполнены:
```text
1. term(C) >= currentTerm(X)
2. votedFor(X) ∈ {null, C}          (ещё не голосовал в этом term'е)
3. up-to-date check (журнал C не хуже журнала X):
      lastLogTerm(C) > lastLogTerm(X)
      ∨ ( lastLogTerm(C) = lastLogTerm(X) ∧ lastLogIndex(C) >= lastLogIndex(X) )
```
При положительном решении: `votedFor := C` (на диск), сброс election timeout, ответ `voteGranted=true`. Иначе `voteGranted=false` со своим `currentTerm`.

### 6.4. Победа и split vote
```text
Кандидат собрал большинство (>= 3) voteGranted=true:
  → role := LEADER
  → для каждого peer: sentLength[peer] := log.length; ackedLength[peer] := 0
  → немедленно разослать heartbeat (пустой AppendEntries) всем
```
Если за свой election timeout кандидат не набрал большинства (split vote) — он снова инкрементирует term и начинает новые выборы. Up-to-date check защищает safety при любом числе раундов.

---

## 7. Log Replication

Протокол строго по семинару 32. Лидер обслуживает клиента, follower проверяет согласованность и дозаписывает.

### 7.1. Приём клиентского запроса
```text
on CLIENT_PUT(command) at node:
  if role = LEADER:
    append LogEntry{term: currentTerm, command} to log   (на диск)
    ackedLength[self] := log.length
    for each follower: ReplicateLog(follower)
    // ответ клиенту OK отправляется, когда commitIndex дойдёт до этой записи
  else:
    ответить NOT_LEADER + leaderHint
```
`get` обслуживается лидером из применённого `kv` (после `lastApplied`). Follower на `get` отвечает `NOT_LEADER` с подсказкой лидера (вариант поведения зафиксировать в README; stale-чтение с follower'а допустимо как осознанный trade-off, но по умолчанию читаем с лидера).

### 7.2. ReplicateLog (на лидере)
```text
function ReplicateLog(follower):
  prefixLen  := sentLength[follower]
  suffix     := log[prefixLen .. log.length-1]
  prefixTerm := (prefixLen > 0) ? log[prefixLen-1].term : 0
  send AppendEntries(term=currentTerm, leaderId=self,
                     prefixLen, prefixTerm, leaderCommit=commitIndex, suffix) to follower
```

### 7.3. Обработка AppendEntries (на follower'е)
```text
on AppendEntries(leaderId, term, prefixLen, prefixTerm, leaderCommit, suffix):
  (сначала универсальное правило term из §5)
  if term = currentTerm: role := FOLLOWER; currentLeader := leaderId; сброс election timeout

  logOk := (log.length >= prefixLen)
        ∧ (prefixLen = 0 ∨ log[prefixLen-1].term = prefixTerm)

  if term = currentTerm ∧ logOk:
    AppendEntriesLocal(prefixLen, leaderCommit, suffix)
    ack := prefixLen + suffix.length
    send AppendEntriesResponse(nodeId=self, term=currentTerm, ack, success=true)
  else:
    send AppendEntriesResponse(nodeId=self, term=currentTerm, ack=0, success=false)
```

`logOk = false` означает одно из двух: у follower'а слишком короткий журнал (`log.length < prefixLen`) либо запись на позиции `prefixLen-1` создана в другом term'е (журналы разошлись раньше точки `prefixLen`).

### 7.4. AppendEntriesLocal — усечение, дозапись, deliver (на follower'е)
```text
function AppendEntriesLocal(prefixLen, leaderCommit, suffix):
  // 1. Усечение при конфликте
  if suffix.length > 0 ∧ log.length > prefixLen:
    index := min(log.length, prefixLen + suffix.length) - 1
    if log[index].term ≠ suffix[index - prefixLen].term:
      log := log[0 .. prefixLen-1]        // выбрасываем конфликтующий хвост (на диск)

  // 2. Дозапись новых записей
  if prefixLen + suffix.length > log.length:
    for i := (log.length - prefixLen) .. (suffix.length - 1):
      append suffix[i] to log              (на диск)

  // 3. Deliver committed записей в KV state machine
  if leaderCommit > commitIndex:
    apply log[commitIndex .. leaderCommit-1] to kv
    commitIndex := leaderCommit; lastApplied := leaderCommit
```
Записи применяются к `kv` **только** после commit, не раньше: до этого момента незафиксированный хвост может быть усечён.

### 7.5. Обработка AppendEntriesResponse (на лидере) + backtrack
```text
on AppendEntriesResponse(follower, term, ack, success):
  if term = currentTerm ∧ role = LEADER:
    if success ∧ ack >= ackedLength[follower]:
      sentLength[follower]  := ack
      ackedLength[follower] := ack
      CommitLogEntries()
    else if sentLength[follower] > 0:
      sentLength[follower] -= 1            // backtrack на одну запись
      ReplicateLog(follower)               // пробуем с меньшим prefixLen
  else if term > currentTerm:
    (универсальное правило: стать FOLLOWER)
```
Проверка `ack >= ackedLength[follower]` отбрасывает устаревшие (поздно пришедшие) ответы, чтобы `ackedLength` не уезжал назад. Backtrack по одной записи за раунд — простой вариант (опт. через быстрый откат по term'у — бонус).

### 7.6. CommitLogEntries (на лидере) — оговорка про term
```text
acks(len) := |{ узел n | ackedLength[n] >= len }|
minAcks   := ceil((5 + 1) / 2) = 3

function CommitLogEntries():
  ready := { len ∈ 1..log.length | acks(len) >= minAcks }
  if ready ≠ ∅ ∧ max(ready) > commitIndex
              ∧ log[max(ready)-1].term = currentTerm:     // ← КЛЮЧЕВАЯ ОГОВОРКА
    apply log[commitIndex .. max(ready)-1] to kv
    commitIndex := max(ready); lastApplied := commitIndex
    ответить OK клиентам, чьи записи попали в новый committed-префикс
```
**Оговорка про term:** лидер фиксирует запись только если запись на границе commit создана в **его текущем term'е**. Запись из старого term'а, даже подтверждённая большинством, фиксируется лишь **транзитом** — когда следующая запись текущего term'а преодолевает большинство, вместе с ней неявно фиксируется весь предшествующий префикс. Без этой проверки два лидера могли бы зафиксировать разные записи на одной позиции и нарушить Agreement.

### 7.7. Leader Completeness (инвариант, который должен соблюдаться)
Новый лидер всегда содержит все committed-записи. Это следствие up-to-date check: committed-запись хранится у большинства `Q_commit`, лидер избран большинством `Q_vote`, `Q_commit ∩ Q_vote ≠ ∅`, и узел из пересечения проголосовал за лидера только потому, что журнал лидера не хуже его собственного. Поэтому в Raft журнал реплицируется **только сверху вниз**: лидер никогда не «учится» у follower'ов.

---

## 8. Persistent state и инварианты записи на диск

`currentTerm`, `votedFor`, `log[]` хранятся в `<data-dir>/<nodeId>/`. Формат на усмотрение студента (например, `meta` для term/votedFor + append-only `log` в JSON Lines). `kv`, `commitIndex`, `lastApplied` на диск **не** пишутся — они восстанавливаются replay'ем журнала при старте.

**Инварианты «log-before-reply» (проверяются на защите через `tail -f` файла журнала параллельно с командами):**
1. `votedFor`/`currentTerm` записаны на диск **до** отправки RequestVoteResponse/любого ответа, меняющего term. Иначе после рестарта узел проголосует дважды в одном term'е → два лидера.
2. Follower записывает `suffix` (и усечение) в `log` на диск **до** отправки `success=true`. Иначе majority перестаёт что-либо гарантировать.
3. Лидер записывает новую запись в `log` на диск **до** рассылки AppendEntries.

**Режим fsync (`--fsync`):**
- `on` (по умолчанию): каждая запись в `log` и каждое обновление `currentTerm`/`votedFor`
  сбрасывается на диск через `FileChannel.force(true)` **до** соответствующего сетевого
  действия — инварианты log-before-reply выше соблюдаются.
- `off`: данные пишутся в файл, но `force()` не вызывается. После краша ОС/потери питания
  незаписанный хвост журнала теряется и инварианты нарушаются (краш самого процесса через
  `kill` данные сохранит — они уже в page cache ОС). Используется только в части C
  бенчмарка как «потолок без durability», не как рабочий режим.

**Старт узла (recovery):** прочитать `currentTerm`, `votedFor`, `log[]` с диска; `role := FOLLOWER`; `commitIndex := 0`, `lastApplied := 0`, `kv := {}` (committed-префикс восстановится из лидера через AppendEntries с `leaderCommit`). Отдельных CLI-команд recovery нет — recovery срабатывает при рестарте процесса с тем же `--data-dir`.

---

## 9. CLI-команды

**Пользовательские:**
- `put <key> <value>` — запись через Raft (CLI находит лидера через `leader`/`leaderHint` и шлёт ему);
- `get <key>` — чтение с лидера;
- `leader` — опросить все узлы и показать, кто считает себя лидером и в каком term'е;
- `status` — таблица по всем узлам: `nodeId, role, currentTerm, commitIndex, log.length`;
- `logDump [--node <id>]` — вывести журнал узла (`index, term, command`).

**Инъекция отказов:**
- `killNode <nodeId>` — отправить `KILL`, узел делает `System.exit` (данные на диске сохраняются);
- `startNode <nodeId>` — заново запустить процесс узла с тем же `--data-dir`;
- `partition <g1Ids> <g2Ids>` — сетевое разделение: узлы группы 1 и группы 2 перестают отвечать друг другу (каждый узел держит in-memory `blockedPeers`, обновляемый через `SET_PARTITION`; заблокированные сообщения молча отбрасываются);
- `healPartition` — очистить `blockedPeers` у всех узлов;
- `setDelay <nodeId> <ms>` — добавить sleep на исходящие RPC узла (для открытия окна под `killNode` в момент репликации).

**Бенчмарк:**
- `bench --puts <N> --threads <T> [--read-ratio <r>]` — прогнать нагрузку, вывести throughput и перцентили latency.

Команды конфигурации таймеров (для части B бенчмарка): `setElectionTimeout <minMs> <maxMs>`.

Режим fsync задаётся флагом запуска --fsync on|off (см. п. 2, п. 8) и фиксируется на весь прогон.
Отдельной CLI-команды нет: durability-политику меняют перезапуском кластера, а не на лету.

---

## 10. Обязательные демонстрационные сценарии

В `demo-scenario.md` — 5 сценариев с копируемыми пошаговыми командами.

### Сценарий 1 — Нормальная работа
Запустить `n1`…`n5`. Дождаться выборов (`leader` показывает одного лидера в одном term'е).
Выполнить `put x 1`, `put y 2`, `get x` → `1`. `status` показывает у всех 5 узлов одинаковый `commitIndex` и `log.length`.
`logDump --node n1` совпадает с `logDump --node n3`.

### Сценарий 2 — Падение лидера
`leader` → пусть лидер `n1`. `killNode n1`. Оставшиеся 4 узла проводят выборы;
`leader` показывает нового лидера в большем term'е. `put z 9` через нового лидера проходит.
`startNode n1` — узел поднимается, через AppendEntries догоняет журнал;
`status` показывает, что `n1` снова видит общий `commitIndex`, а пропущенные записи появились в его `logDump`.

### Сценарий 3 — Split vote / разделение кворума
`partition n1,n2 n3,n4,n5`. Меньшая группа (`n1,n2`) не набирает большинства (2 < 3): её кандидаты бесконечно инкрементируют term без победы — лидера нет, `put` через них даёт `NO_LEADER`/`TIMEOUT`.
Большая группа (`n3,n4,n5`) выбирает лидера и обслуживает `put`. `healPartition` → кластер сходится: отставшая группа принимает лидера с бо́льшим term'ом, журналы выравниваются, `status` снова единый.

### Сценарий 4 — Расходящийся журнал
`setDelay <leaderId> 4000`, затем `put a 1`: лидер начинает репликацию, часть follower'ов получает запись, часть — нет (из-за задержки).
В окне задержки `killNode <leaderId>`. Новый лидер избирается; у follower'ов журналы разной длины.
Через `logDump` до и после показать, что новый лидер делает backtrack до общего префикса, усекает конфликтующие хвосты и приводит все журналы к согласованному виду.
Подчеркнуть: усечённые записи никогда не были committed.

### Сценарий 5 — Параллель с Paxos (концептуальный, без кода)
После любого из сценариев 1–4 через `logDump`/`status` показать состояние журналов и устно объяснить соответствие:
где здесь аналог «кворума обещаний» (RequestVote + majority + up-to-date check ↔ Prepare/Promise + `max n_a`),
а где «фаза принятия» (AppendEntries + commit ↔ Accept!/Accepted), и почему committed ↔ chosen.

В `demo-scenario.md` достаточно дать **план объяснения** (тезисно), полный разбор — устно на защите.

---

## 11. Бенчмарки и `report.md`

### 11.1. Что измеряем
`throughputOpsSec`, `avgMs`, `p50Ms`, `p95Ms`, `p99Ms`; для части B — `failoverMs` (время от падения лидера до первого успешного `put` через нового лидера). Результаты — в `benchmarks/results.csv`.
```text
part,scenario,liveNodes,electionTimeoutMs,fsync,threads,totalOps,throughputOpsSec,avgMs,p50Ms,p95Ms,p99Ms,failoverMs
```

### 11.2. Обязательные прогоны (минимум 7)

**Часть A — throughput vs число живых узлов (3 прогона).** Кластер 5 узлов, нагрузка 100% `put`, `threads=100`, `totalOps=10000`.
- все 5 живы;
- убит 1 (кворум = 4 из 5, ещё работает);
- убиты 2 (кворум = 3 — минимальный для majority).
Цель: throughput меняется с числом узлов, но система продолжает фиксировать записи, пока живо большинство.

**Часть B — election timeout vs время обнаружения отказа (2 прогона).** Убить текущего лидера, замерить `failoverMs` до первого успешного `put` через нового лидера.
- election timeout `150–300 мс` (стандартный);
- election timeout `500–1000 мс` (увеличенный).
Цель: trade-off — меньший timeout даёт быстрее восстановление, но повышает риск ложных выборов.

**Часть C — стоимость персистентности (2 прогона).** Кластер 5 узлов, 100% put, threads/totalOps как в части A. Сравнить latency и throughput put при:
(a) --fsync on — force() на каждую запись журнала (рабочий режим);
(b) --fsync off — без force() (небезопасно).

Цель: количественно показать стоимость durability.


### 11.3. Требования к `report.md`
Таблица всех прогонов; на каждую часть обязательно минимум по 1 графику, итого 3 в сумме (throughput vs liveNodes; `failoverMs` vs electionTimeout; latency put с `fsync` vs без). Объяснения:
1. почему throughput и latency меняются при убийстве узлов и почему 2 убитых — предел (кворум = 3);
2. почему меньший election timeout ускоряет failover, но грозит лишними выборами и disruption;
3. во сколько раз `fsync` дороже и почему без него алгоритм некорректен после краша.

---

## 12. Формат сдачи

В новом репозитории:
1. **`README.md`:** архитектура (5 узлов как процессы + CLI), формат persistent state и инвариантов log-before-reply, спецификация протокола, инструкция запуска (`--id`, `--port`, `--data-dir`, peers), семантика `get` (leader-read или stale follower-read), базовые промпты для LLM, использованных в решении.
2. **`demo-scenario.md`:** копируемые пошаговые команды для всех 5 сценариев, включая `killNode`/`startNode` и `partition`.
3. **`report.md` + `benchmarks/results.csv`** + графики.
4. Структура репозитория на усмотрение студента; ожидается разделение на модули `node / election / replication / storage / network / cli`.
5. Открытый Pull Request в `master`/`main`.

---

## 13. Оценивание

### 13.1. Функциональность — 8 баллов

| Блок                     | Баллы | Что требуется                                                                                                                   |
|--------------------------|------:|---------------------------------------------------------------------------------------------------------------------------------|
| Базовая инфраструктура   |   1.0 | 5 узлов и CLI как отдельные процессы; TCP, JSON Lines; `put/get/status/leader/logDump` работают на живом кластере               |
| Leader Election          |   2.0 | randomized election timeout; RequestVote + up-to-date check; majority → leader + немедленный heartbeat; split vote → новый term |
| Log Replication          |   2.5 | AppendEntries; `logOk`; усечение + дозапись; backtrack при `success=false`; commit по majority с **оговоркой про term**         |
| Persistent state         |   1.0 | `currentTerm`, `votedFor`, `log[]` на диске; log-before-reply соблюдён; рестарт восстанавливает узел из журнала                 |
| Сценарии + наблюдаемость |   0.5 | работают `killNode/startNode`, `partition/healPartition`, `setDelay`; `logDump`/`status` корректно показывают расхождения       |
| Бенчмарки + `report.md`  |   1.0 | выполнены ≥7 прогонов; CSV сохранён; `report.md` содержит ≥3 графика и объяснения trade-offs                                    |

### 13.2. Качество кода — 2 балла

**1. Архитектура — 1.0.**

Чёткое разделение слоёв: state machine ролей, репликация, persistence и сеть не смешаны;
модели сообщений и доменные сущности выделены;
зависимости от инфраструктуры (persistent log, сеть) идут через интерфейсы (например, `interface DurableState { saveMeta(...); appendLog(...); readAll(); }`), чтобы в тестах подменять файловый слой на in-memory.
Нет «больших классов» и spaghetti-code.

**2. Concurrency и читаемость — 1.0.**
Потокобезопасность: таймеры, входящие RPC и запись на диск идут конкурентно — нужны корректные блокировки, нет гонок и дедлоков;
таймауты на всех сетевых вызовах; осмысленные логи (видно текущий term, роль, отправленные/полученные RPC, продвижение `commitIndex`);
единый стиль, хорошие имена.

---

## 14. Бонусные баллы (суммарно ≤ 10)

Бонус может компенсировать недобор по функциональности, но итог не превышает 10.

- **(+1.0)** Оперативная сдача до 2026-06-27, 20:00, использование выполненной домашки для подготовки к экзамену
- **(+0.5)** Быстрый backtrack: при `success=false` follower возвращает `conflictTerm`/`conflictIndex`, лидер откатывается сразу к границе term'а, а не по одной записи. В `report.md` — сравнение числа раундов синхронизации сильно отставшего follower'а.

---

## 15. Checklist перед защитой

- [ ] 5 узлов и CLI запускаются как отдельные процессы, у каждого узла свой `--data-dir`;
- [ ] стартовая роль — FOLLOWER; срабатывает универсальное правило term (бо́льший term → FOLLOWER + сброс `votedFor`);
- [ ] election timeout случайный из настраиваемого диапазона, сбрасывается heartbeat'ом;
- [ ] RequestVote учитывает up-to-date check; в одном term'е голос отдаётся не более одного раза;
- [ ] победитель шлёт heartbeat немедленно; split vote приводит к новому term'у;
- [ ] AppendEntries: `logOk` вычисляется корректно; при `false` — `success=false`;
- [ ] follower усекает конфликтующий хвост и дозаписывает `suffix`; применяет к `kv` только committed;
- [ ] лидер делает backtrack по `sentLength` и доводит отставшего follower'а до согласия;
- [ ] commit идёт по majority и соблюдает **оговорку про term** (запись старого term'а фиксируется только транзитом);
- [ ] `currentTerm`/`votedFor` записаны на диск до ответа; `log` записан до `success`/рассылки;
- [ ] `killNode` → `startNode` восстанавливает узел из журнала, он догоняет кластер;
- [ ] `partition`/`healPartition` воспроизводят разделение кворума и последующую сходимость;
- [ ] `setDelay` открывает окно для расходящегося журнала (сценарий 4);
- [ ] выполнены ≥7 прогонов бенчмарка, есть CSV и ≥3 графика в `report.md`;
- [ ] студент умеет объяснить аналогию **RequestVote ↔ Prepare/Promise** и **AppendEntries ↔ Accept!/Accepted**.

---

## 16. Сдача ДЗ преподавателю

1. Запись в Google Calendar на проверку ДЗ [по ссылке](https://calendar.app.google/z9GWcWG3jb8wWSoZ9). Записываться на слот необходимо не позднее, чем за 1 час до слота.
2. Краткий созвон с преподавателем:
   - демонстрация функциональности работы приложения;
   - защита работы и ответы на вопросы по работе;
   - ответы на дополнительные вопросы;
   - выставление оценки за домашнюю работу по 10-балльной шкале.
3. Soft дедлайн сдачи преподавателю (для +1 балла): 2026-06-27, 20:00.
4. Hard Дедлайн сдачи преподавателю: 2026-07-01, 20:00.
