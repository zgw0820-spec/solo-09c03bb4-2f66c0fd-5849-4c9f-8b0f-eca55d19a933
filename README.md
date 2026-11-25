# 聚合口摘链演练（FastAPI 本地版）

在本地模拟“从聚合口摘掉一根成员链路”的值班操作：两条 **FIFO** 成员链路（A/B）
的送达延时与确认延时都可调，按流发送带编号的报文，申请摘链时设置等待时限和
缓冲上限，然后观察对端收包顺序、每包实际走的链路，以及本次摘链的最终结果。

## 规则（与演练行为一一对应）

- **发送窗口（每条成员链路 1～64 包，默认 1）**：窗口上限表示“已发出但还没收到
  确认”的包数。有空位时，链路按原队列顺序继续发包；`窗口=1` 时行为与旧版严格
  串行 FIFO 完全一致（发一包 → 送达 → 等确认 → 才发下一包）。值班员可以把确认
  延时调大、再调大窗口并连续发包，直接对比多包在途带来的吞吐提升。
- **耗时取值时机**：每包的传输（送达）耗时取它**发出那一刻**的
  `delivery_delay_ms`，发出后再改延时不影响该包；确认等待从该包**物理送达**
  开始，`ack_delay_ms` 取送达当时的设置。确认一回来就把该包移出在途集合、
  释放一个窗口名额，并立刻按顺序补发队列里的下一包。
- **在途可查**：`GET /api/links` 每条链路都列出 `window_size`（上限）、
  `in_flight_count`（已发未确认数量）和 `in_flight_packet_ids`（已发未确认的
  报文 ID，按发出顺序），另有 `queue_depth`（还没轮到发出的排队包）和
  `in_flight_packet_details`（每包状态/采用的延时/时间戳）。
- **乱序到达、顺序交付**：窗口 >1 时多包真正并行在途。后发出的包可能因为发出时
  采用了更短的送达延时而**先物理传完**，但它会进入链路重排缓冲，必须等同一条
  链路上更早发出的包先交给收端，`GET /api/flows/{id}/received` 看到的顺序因此
  永远与链路发出顺序一致。先到的包仍按自己的送达时间启动确认，所以可能出现
  “后发包先确认”。
- **在线调窗口，调低立即生效**：`PATCH /api/links/{A|B}`（或
  `PUT/POST/PATCH /api/links/{A|B}/window`）随时改 `window_size`。调大立刻
  补满空位；调低不会取消任何已发包，它们继续送达/等确认，只有当在途数**严格
  低于新上限**时才继续补发。`POST /api/reset` 清空所有流、在途包和申请，
  **窗口恢复为默认 1**。
- **摘链确认快照**：摘链申请创建时，把成员链路上**当时已排队/在途**的受影响流
  报文记为一个快照。必须等快照里**每一个**包都确认后才迁流——即使后发的包先
  确认，也不能提前切链。
- **等待期间**：待迁流的新包进入缓冲；缓冲总数达到上限就返回 `429` 拒收，**其他流
  照常发送**。被拒的编号没有被接纳，摘链结束后可以重发。
- **超时**：到达等待时限仍未收齐确认，本次摘链失败（`timed_out`），流不迁移，
  缓冲的报文按接纳顺序回到**原链路**，并按该链路当前窗口争抢名额发送。之后才到
  的确认会被记录，但**不会改变**本次结果。
- **一次只处理一个摘链申请**：已有 pending 申请时，第二个申请返回 `409`。
- 摘链成功后，成员链路从聚合组中摘除（`removed: true`），不能再在它上面建流；
  此时两条链路只剩一条，无法再摘，直到 `POST /api/reset` 复原。

## 启动

```bash
docker compose up --build -d
# 打开 http://localhost:8000   （端口可用 DRILL_PORT=9000 覆盖）
```

容器只有一个 worker（状态在内存里），健康检查打 `/healthz`。初始延时可通过
`.env`（参考 `.env.example`）或启动后 `PATCH /api/links/{A|B}` 调整。

不想用 Docker 时可以直接跑：

```bash
python3 -m pip install -r requirements.txt
python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
```

## 可直接调用的演练请求

以下命令假设服务在 `http://localhost:8000`，按顺序执行即可复现一次成功摘链。
时间单位均为毫秒。

```bash
BASE=http://localhost:8000

# 0) 复位（清空流、申请和事件）
curl -s -X POST $BASE/api/reset

# 1) 把链路延时调小，方便观察（可选）
curl -s -X PATCH $BASE/api/links/A \
  -H 'Content-Type: application/json' \
  -d '{"delivery_delay_ms": 300, "ack_delay_ms": 200}'
curl -s -X PATCH $BASE/api/links/B \
  -H 'Content-Type: application/json' \
  -d '{"delivery_delay_ms": 150, "ack_delay_ms": 80}'

# 2) 建两条流：voice 走 A（待摘），video 走 B
curl -s -X POST $BASE/api/flows \
  -H 'Content-Type: application/json' -d '{"flow_id":"voice","link":"A"}'
curl -s -X POST $BASE/api/flows \
  -H 'Content-Type: application/json' -d '{"flow_id":"video","link":"B"}'

# 3) voice 先发两包，让它们排在 A 上
curl -s -X POST $BASE/api/flows/voice/packets \
  -H 'Content-Type: application/json' -d '{"seq":1,"payload":"v1"}'
curl -s -X POST $BASE/api/flows/voice/packets \
  -H 'Content-Type: application/json' -d '{"seq":2,"payload":"v2"}'

# 4) 申请摘 A：最多等 5s，缓冲上限 5 包；记下 removal_id
RM=$(curl -s -X POST $BASE/api/removals \
  -H 'Content-Type: application/json' \
  -d '{"member_link":"A","wait_ms":5000,"buffer_limit":5}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["removal_id"])')
echo "removal_id=$RM"

# 5) 等待期间给 voice 再发一包 -> 202，进入缓冲
curl -s -X POST $BASE/api/flows/voice/packets \
  -H 'Content-Type: application/json' -d '{"seq":3}'

# 6) 其他流不受影响
curl -s -X POST $BASE/api/flows/video/packets \
  -H 'Content-Type: application/json' -d '{"seq":1}'

# 7) 再申请一次摘链 -> 409（一次只处理一个）
curl -s -X POST $BASE/api/removals \
  -H 'Content-Type: application/json' \
  -d '{"member_link":"B","wait_ms":1000,"buffer_limit":1}'

# 8) 等对端确认（A 上两包依次 300+200ms）后查看结果：
#    status=succeeded, moved_flow_ids=["voice"], flushed_to="B", snapshot_acked=2
sleep 1.2
curl -s $BASE/api/removals/$RM | python3 -m json.tool

# 9) 对端收包顺序：1,2 走 A，3 走 B，按 1,2,3 送达
sleep 0.3
curl -s $BASE/api/flows/voice/received | python3 -m json.tool
```

## 发送窗口实验：确认延时大时，多包在途能否提高吞吐

下面这组请求可直接复制。思路：把确认延时调大（例如 400ms），先发一串包，
分别用窗口 1 和窗口 8 对比“全部交给收端”的耗时；发送后立刻查链路状态，
能看到上限和已发未确认的包。

```bash
BASE=http://localhost:8000

# 0) 复位：清空在途包，窗口回到 1
curl -s -X POST $BASE/api/reset

# 1) 调窗口（1~64，默认 1）。以下几种写法等价：
curl -s -X PATCH $BASE/api/links/A \
  -H 'Content-Type: application/json' -d '{"window_size":8}'
curl -s -X PATCH $BASE/api/links/A \
  -H 'Content-Type: application/json' -d '{"window":8}'
curl -s -X PUT  $BASE/api/links/A/window \
  -H 'Content-Type: application/json' -d '{"window_size":8}'
curl -s -X POST "$BASE/api/links/A/window?window=8"
# 0 或 65 会返回 422。

# 2) 制造“大确认延时”：送达 30ms，确认 400ms
curl -s -X PATCH $BASE/api/links/A \
  -H 'Content-Type: application/json' \
  -d '{"delivery_delay_ms":30,"ack_delay_ms":400,"window_size":1}'
curl -s -X POST $BASE/api/flows \
  -H 'Content-Type: application/json' -d '{"flow_id":"bench","link":"A"}'

# 3) 窗口=1 时连续发包（串行：每包要等确认回来才发下一包）
time (for i in $(seq 1 8); do
  curl -s -X POST $BASE/api/flows/bench/packets \
    -H 'Content-Type: application/json' -d "{\"seq\":$i}" >/dev/null
done)
sleep 3.5
curl -s $BASE/api/flows/bench/received | python3 -c \
  'import json,sys; d=json.load(sys.stdin); print("received", d["count"], [x["seq"] for x in d["delivery_order"]])'

# 4) 复位后把窗口调到 8，再连续发 8 包：8 包同时在途，约一个送达+确认周期完成
curl -s -X POST $BASE/api/reset >/dev/null
curl -s -X PATCH $BASE/api/links/A \
  -H 'Content-Type: application/json' \
  -d '{"delivery_delay_ms":30,"ack_delay_ms":400,"window_size":8}'
curl -s -X POST $BASE/api/flows \
  -H 'Content-Type: application/json' -d '{"flow_id":"bench","link":"A"}'

# 连续发包（shell 循环；也可以用 & 并发打满）
for i in $(seq 1 8); do
  curl -s -X POST $BASE/api/flows/bench/packets \
    -H 'Content-Type: application/json' -d "{\"seq\":$i}" >/dev/null
done

# 5) 查状态：window_size=8、in_flight_count=8，并列出 8 个已发未确认包 ID
curl -s $BASE/api/links | python3 -m json.tool
curl -s $BASE/api/state  | python3 -m json.tool   # 含链路/流/事件总览

# 6) 收端顺序仍是 1..8
sleep 1
curl -s $BASE/api/flows/bench/received | python3 -m json.tool
```

观察“后发先至但按序交付”：窗口设 2，先把 A 的送达延时调到 600ms 发 seq=1，
再把送达延时改成 40ms 发 seq=2。seq=2 约 40ms 就物理到达（并开始等确认），
但在 seq=1 于约 600ms 到达前，`/api/flows/bench/received` 里不会出现任何包；
两包最终按 `1,2` 顺序交给收端。包详情里可以对比 `arrived_at`（物理到达）、
`handed_at`（按序交付收端）和 `acked_at`：

```bash
curl -s -X POST $BASE/api/reset >/dev/null
curl -s -X PATCH $BASE/api/links/A -H 'Content-Type: application/json' \
  -d '{"delivery_delay_ms":600,"ack_delay_ms":80,"window_size":2}'
curl -s -X POST $BASE/api/flows -H 'Content-Type: application/json' \
  -d '{"flow_id":"reorder","link":"A"}' >/dev/null
P1=$(curl -s -X POST $BASE/api/flows/reorder/packets -H 'Content-Type: application/json' -d '{"seq":1}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["packet"]["packet_id"])')
curl -s -X PATCH $BASE/api/links/A -H 'Content-Type: application/json' -d '{"delivery_delay_ms":40}' >/dev/null
P2=$(curl -s -X POST $BASE/api/flows/reorder/packets -H 'Content-Type: application/json' -d '{"seq":2}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["packet"]["packet_id"])')
sleep 0.2
curl -s $BASE/api/flows/reorder/received          # count=0：seq2 先到也不能上交
curl -s $BASE/api/packets/$P2 | python3 -m json.tool
sleep 1
curl -s $BASE/api/flows/reorder/received          # 顺序 1,2
```

调低窗口立即生效：窗口 4 且 4 包在途时 `PATCH {"window_size":1}`，这 4 包继续
处理，第 5 包留在 `queue_depth` 里；只有在途数随确认降到 0（严格低于新上限 1）
后才会发出。`POST /api/reset` 随时清空在途并把窗口重置为 1。

超时场景：把第 4 步的 `wait_ms` 调成比 `delivery+ack` 小（例如 `50`），再发第 5 步。
结果会是 `timed_out`、`moved_flow_ids: []`、`flushed_to: "A"`，之后迟到的确认
只体现在 `late_confirmations_ignored`。

缓冲上限场景：把 `buffer_limit` 设成 `1`，等待期间发两包，第二包返回 `429`。
被拒的 seq 未接纳，摘链完成后可以重发。

### 一键跑完整演练脚本

```bash
# 本机已有服务时
python3 drills/run_drill.py --base-url http://localhost:8000
# 或让 Compose 起一个一次性客户端（跑完即退出）
docker compose --profile drill run --rm drill-client
# 只跑某个场景：--only success|timeout|buffer|after_removal|window
```

脚本只依赖标准库，五个场景全部通过时返回 0，否则返回 1 并列出失败项。
`window` 场景会校验默认窗口 1、1～64 边界、在途包列表、窗口 1 与窗口 4 的
吞吐差异，以及“后发先至仍按序交付”。

## 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 健康检查 |
| GET | `/api/links` | 两条链路的延时、`window_size` 上限、`in_flight_count`/`in_flight_packet_ids` 已发未确认包、队列深度、收发计数、是否已摘除 |
| PATCH | `/api/links/{A\|B}` | 调整 `delivery_delay_ms` / `ack_delay_ms` / `window_size`（1～64） |
| PUT/POST/PATCH | `/api/links/{A\|B}/window` | 只调窗口，如 `{"window_size":8}` 或 `?window=8` |
| GET | `/api/flows` | 所有流及当前承载链路、已接纳/已送达计数 |
| POST | `/api/flows` | 建流 `{flow_id?, link}` |
| POST | `/api/flows/{id}/packets` | 发包 `{seq, payload?}`；正常入队 201，进缓冲 202，缓冲满 429，重复 seq 409 |
| GET | `/api/flows/{id}/received` | 对端收包顺序（序号、报文 ID、链路、时间） |
| GET | `/api/packets/{packet_id}` | 单个报文的状态与走过的链路 |
| POST | `/api/removals` | 申请摘链 `{member_link, wait_ms, buffer_limit}`；需等待 202，立即完成 200 |
| GET | `/api/removals` | 所有申请（倒序摘要） |
| GET | `/api/removals/{id}` | 单个申请详情：快照、确认进度、缓冲、拒收、迟到确认、事件时间线 |
| GET | `/api/state` | 链路 + 流 + 当前申请 + 最近事件 |
| POST | `/api/reset` | 清空并重建两条链路 |

交互界面在 `http://localhost:8000/`，包含链路延时调节、建流/发包、摘链申请、
对端收包顺序、事件日志，每 0.5s 自动刷新。

## 目录

```
app/main.py             FastAPI 路由与静态页挂载
app/engine.py           演练核心：发送窗口、在途/重排、流、报文、摘链状态机
app/config.py           从环境变量读取初始链路延时
app/static/index.html   值班界面（无外部依赖）
drills/run_drill.py     端到端演练脚本（仅标准库）
tests/test_drill.py     pytest 测试
Dockerfile / docker-compose.yml
```

## 测试

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -q
```

`tests/test_drill.py` 覆盖原有 FIFO/摘链流程，`tests/test_window.py` 覆盖发送
窗口：默认值与字段、1～64 校验、在途列表与确认释放名额、窗口封顶与按序补发、
吞吐提升、乱序到达按序交付、调低窗口立即生效、摘链等待全部快照确认、复位归 1。

## 已知边界（本地演练的取舍）

- 状态全部在内存，重启容器即清空；只适合演练，不作持久化。
- 窗口内的每个在途包各用一个 `asyncio` 任务计时（每链路上限 64 个），发送/确认
  延时是模拟值；链路侧用按发出序号的重排缓冲保证按序交给收端，没有真实网络
  丢包/乱序。
- 窗口名额在**确认返回时**才释放；收端按序交付只影响
  `/api/flows/{id}/received` 的顺序，不推迟各包自己的确认计时，因此可能出现
  “后发包先确认、但仍排队等前包交付”的可观察状态。
- “对端确认”由服务内部模拟：快照报文全部被确认即视为对端收齐；没有独立的对端进程。
- 超时判定是精确的一次性定时器，需等待时长与链路延时同量级时，边界上偶发的
  先后顺序属于正常现象。
