# 事务交错推演（Transaction Interleaving Playground）

用 Python + FastAPI + 原生 HTML/CSS/JavaScript 构建的事务调度可视化网页：
编辑初始键值与 2～3 条事务脚本，系统按**轮转调度**（依次从各事务队头取操作）
排出同一份全局调度，再分别在三种隔离级别下生成确定性轨迹并逐步展示。

## 隔离级别语义

| 隔离级别 | 读取规则 |
|---|---|
| **Read Uncommitted (RU)** | 读键上的**最新写入**，包括其他事务尚未提交的版本；写入方若随后 ABORT，该读列入「脏读清单」 |
| **Read Committed (RC)** | 每次读只取键上**最新已提交版本**——“最新”按**提交顺序**而非写入顺序（先写者可以晚提交，晚提交者即为当前版本）；不读未提交数据，亦不看自身未提交的写 |
| **Snapshot Isolation (SI)** | 读 **BEGIN 时的快照**，自身写入优先；BEGIN 之后同键若已被其他事务提交，提交时按**首提交者胜**整笔回滚 |

页面支持：

- 逐步 / 自动播放每一条调度操作，展示**读值、来源版本、写入者**与逐条解释；
- 每一步刷新**版本链**（初始 / 未提交 / 已提交 / 已回滚版本，读写位置高亮）与事务状态；
- 按**已提交事务的真实读写关系**生成三种依赖边：
  - `WR`：Tj 读到 Ti 已提交版本（Ti → Tj）；
  - `WW`：同键上 Ti 先写、Tj 后写且均提交（Ti → Tj，**按真实写入顺序**，与提交先后无关）；
  - `RW`（反依赖）：Ti 读过某版本**之后**，同键才有 Tj 提交的新版本（Ti → Tj；只读时已经提交的版本，即使版本编号更大，也不构成反依赖）；
  - 图中发现环时，环上的边与对应的读/写操作红色高亮；
- 单列「**脏读清单**」：读到后来被 ABORT 的未提交数据；
- **调度对照**：输入由 T1/T2/T3 组成的完整执行顺序（每次取对应事务尚未执行的队头操作），
  与默认轮转顺序分别独立推演 RU / RC / SI，按（事务名, 事务内操作序号）对齐
  READ / COMMIT / ABORT，依默认轮转的步序标出读值、来源类型、写入者或结束状态的
  首次差异，并并排展示两侧脏读清单与依赖图是否成环；可切换查看两侧逐步轨迹；
- 内置四个一键加载示例：脏读、丢失更新（对比 RC 提交成环与 SI 回滚）、写偏序（SI 下双双提交但 RW 成环）、同键交错提交（见下）。

## 交错示例：同键先写者晚提交（RC 按提交顺序取版本）

初始 `x=0`（另设 `a=b=c=0` 仅用于拉开调度），三条事务经轮转调度得到如下交错——
**T1 先写 `x=1`，却晚于写 `x=2` 的 T2 提交；T3 在两次提交都完成之后读 x**：

| 步 | 操作 | 说明 |
|---|---|---|
| 1–3 | T1 / T2 / T3 `BEGIN` | 三个事务先后开始 |
| 4 | T1 `WRITE x 1` | 版本链上先生成 v(x=1)，未提交 |
| 5 | T2 `WRITE x 2` | 版本链上再生成 v(x=2)，未提交（**版本链保持写入顺序**） |
| 6–7 | T3 `READ b`；T1 `READ a` | 与 x 无关的填充操作 |
| 8 | T2 `COMMIT` | x=2 先成为已提交版本 |
| 9 | T3 `READ c` | 填充操作 |
| 10 | T1 `COMMIT` | x=1 **后提交**，成为按提交顺序最新的已提交版本 |
| 11 | T3 `READ x` | 三种隔离级别结果不同（见下） |
| 12 | T3 `COMMIT` | |

第 11 步 T3 读 x 的结果：

| 隔离级别 | 读值 | 来源 |
|---|---|---|
| **RU** | **2** | 继续取版本链上最新写入（此时为 T2 的 x=2） |
| **RC** | **1** | T1 的提交是最后一次提交——即使 v(x=1) 的版本编号更小、在链上更旧，它才是最新已提交版本 |
| **SI** | **0** | T3 读 BEGIN 快照；且 T1 写入的 x 在快照后已被 T2 提交，T1 于第 10 步提交时同键冲突，**整笔回滚**，x=1 作废 |

补充：第 8 步 T2 提交之前，任何事务在 RC 下读 x 都只能读到初始值 0
（不读未提交值，也不读自身未提交的写）；回滚产生的版本在三种级别下均不可读。

依赖图（只含已提交事务，回滚事务不参与）：

- RC / RU：`T1 → T3`（或 RU 下的 `T2 → T3`）为 **WR**；`T1 → T2` 为 **WW**（真实写入顺序）；
  T3 读 x 时两个版本都**已经提交**，所以**不会**仅因 T2 的版本编号更大而多出 `T3 → T2` 的 RW 边；
- SI：T1 回滚不参与图；T3 读快照且没有后于其读的提交，无 WR / RW 边。

页面点击「**示例4 · 同键交错提交**」即可一键加载该场景。

## 事务脚本语法

每条脚本一行，仅允许以下操作（大小写不敏感）：

```
BEGIN
READ key
WRITE key value
COMMIT
ABORT
```

- 必须以 `BEGIN` 开头，以 `COMMIT` 或 `ABORT` 结尾，结束语句之后不能再有操作；
- 值支持整数、小数、`true` / `false` / `null` 与字符串；
- 初始键值每行一个 `key=value`；
- 勾选「启用 T3」可使用 3 条事务。

## 一键启动（Docker Compose）

```bash
docker compose up --build
```

启动后访问：

- **http://localhost:8000** —— 推演网页
- http://localhost:8000/api/health —— 健康检查

后台停止：`docker compose down`。

## 本地直接运行（可选，无需 Docker）

需要 Python 3.11+：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

然后访问 http://localhost:8000 。

## 接口

`POST /api/simulate`

```json
{
  "initial": "x=0\ny=0",
  "txns": [
    "BEGIN\nREAD x\nWRITE x 10\nCOMMIT",
    "BEGIN\nREAD x\nWRITE x 20\nCOMMIT"
  ]
}
```

返回三种隔离级别各自的 `steps`（含每步版本链快照、读值来源、事务状态）、
`dirty_reads` 与 `graph`（节点 / WR·WW·RW 边 / 环路径）。

`POST /api/compare`（调度对照：自定义执行顺序 vs 默认轮转）

```bash
curl -s -X POST http://localhost:8000/api/compare \
  -H "Content-Type: application/json" \
  -d '{
    "initial": "x=0",
    "txns": [
      "BEGIN\nWRITE x 1\nABORT",
      "BEGIN\nREAD x\nCOMMIT"
    ],
    "order": ["T1", "T1", "T1", "T2", "T2", "T2"]
  }'
```

上例中默认轮转会让 T2 在 RU 下读到 T1 未提交的 `x=1`（随后 T1 回滚，计入脏读清单）；
自定义顺序把 T1 的写入与回滚全部提前，T2 只读到初始值 `0`——响应 `comparison.RU.first_diff`
即标出这一首次差异（默认第 4 步 vs 自定义第 5 步），RC / SI 则 `identical=true`。

- `order` 由 T1/T2/T3 组成（元素内可用空格 / 逗号分隔多个，大小写不敏感），
  调度时每次取对应事务尚未执行的队头操作；
- 顺序包含未知事务、或某事务出现次数与其脚本操作数不符时返回 `400` 及错误详情；
- 响应中 `default` / `custom` 两侧各自包含三种隔离级别的完整结果
  （结构与 `/api/simulate` 的 `results` 相同，两侧完全独立运行——SI 因顺序变化
  回滚时，后续读值、版本链与依赖图均以各侧实际结果为准）；
- `comparison` 按隔离级别给出对照结论：`first_diff` 为依默认轮转诊序对齐
  READ / COMMIT / ABORT 后的**首次差异**（READ 比较读值、来源类型、写入者；
  COMMIT / ABORT 比较结束状态；无差异时 `identical=true`、`first_diff=null`），
  另含两侧 `dirty_reads` 清单与依赖图是否成环（`graph.*_has_cycle` 及环路径）。
