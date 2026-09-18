# 协作白板 v2 · CRDT 多人并发内核

在 v1「单房间单笔迹、服务端 seq 排序」的基础上，升级为支持**多用户并发编辑、对象化白板、选择性撤销**的协作内核。
原生 HTML/CSS/JS + Canvas 前端，Node.js + ws 后端，零额外依赖（内核同时被浏览器与 Node 加载）。

## 目录结构

```
whiteboard-sync/
├── package.json
├── server.js            # 房间 / 权威 CRDT 物化 / 因果缓冲 / 快照 / 日志压缩 / 心跳
├── public/
│   ├── kernel.js        # ★ 共享协作内核（CRDT、时钟、撤销、压感、平滑、橡皮、识别）
│   ├── app.js           # 对象化渲染 / 指针手势 / 工具栏 / 网络层
│   ├── index.html
│   └── style.css
├── test-kernel.js       # 259 项内核一致性测试（并发/选择性撤销/事务/压缩/压感/橡皮…）
├── test-smoke.js        # 27 项真实 ws 协议测试（自动拉起服务端，覆盖 6 个验收场景）
└── test-frontend.js     # 21 项前端状态机测试（stub DOM/Canvas/WebSocket 加载真实 app.js）
```

## 启动与测试

```bash
npm install
npm start                 # http://localhost:8080/

npm test                  # 三套测试全跑
node test-kernel.js       # 纯内核（无需服务端）
node test-smoke.js        # 协议（未运行时自动拉起 server.js）
node test-frontend.js     # 前端（无需浏览器）
```

## 一、一致性模型（需求 1、3）

- **LWW-Element-Map CRDT**：白板是一组对象（`oid`），每个字段是一个 LWW 寄存器，
  写入带 `(lamport, clientId)` 时间戳。任意副本对同一组信封折叠出**相同结果**，
  与网络到达顺序无关 —— 这就是「不能只靠服务端 seq 排序」的核心：**seq 仅用于日志排序/观测，不参与冲突仲裁**。
- 每个信封携带：
  - `clientId`：操作者；
  - `lamport`：Lamport 逻辑时钟；
  - `clock`：版本向量（依赖向量），声明「我见过各节点的第几条操作」；
  - `id`：`<clientId>:<localCount>`，全局幂等。
- **因果投递**：`CausalBuffer` 用 happens-before 规则（发送者序号连续 + 依赖向量不超前）
  保证 `create` 必先于后续 `set`；缺依赖的信封先挂起，补齐后按序冲刷。

## 二、对象模型与操作类型（需求 2）

对象类型：`stroke / rect / ellipse / triangle / arrow / line / text / note / image / group`。
操作 `kind`：`create / set / delete / restore / group / ungroup / layer / erase`，
覆盖图形、文本、便签、图片、选择、移动、缩放、旋转、删除、图层调整、组合、解组。

- 移动/缩放/旋转统一写对象的仿射变换字段 `tr{tx,ty,sx,sy,r}`，对笔迹和图形一视同仁。
- 图层顺序用**十进制分数索引**（fractional indexing）：`zBetween(a,b)` 逐位长除取严格中点，
  并发同位置插入产生相同键，再由 `(lamport,clientId)` 确定平局，永不重排。

## 三、选择性撤销（需求 4，验收场景 2）

撤销**不回滚历史**，而是发一条「逆操作」信封（`op.inv = {originId, originLamport, polarity}`）。
内核折叠逆写入时做**架空检测**：

> 若原操作之后（含并发更大 lamport）存在**他人的普通写入**到同一字段，逆操作空转（void）。
> 他人的撤销/重做不算新鲜意图；自身的记录不阻塞；保护是**字段级**的（`create` 的撤销是对象级，
> **像素擦除的撤销是擦除单元级**：同一分块内他人只擦了别的单元时，自己的撤销对未冲突单元仍然生效，
> 只有被他人重擦过的单元才空转）。

因此：
- 只撤销自己的操作（`UndoManager` 只记录本人发出的顶层编辑/事务）；
- A 撤销自己旧笔迹时，若 B 已修改该区域，**A 的撤销不会覆盖 B 的结果**；
- 没被他人碰过的字段正常恢复，互不影响（per-field）。
- 历史面板支持对任意一条自己的历史操作做**选择性撤销**（`undoSelective`），不限于栈顶。

## 四、事务与原子操作组（需求 5，验收场景 3）

多对象编辑（一次粘贴多个元素、一次移动多个选中对象）的多个信封用 `WB.atomic()` 绑定同一 `txnId`。
因果缓冲保证整组要么一起就绪、要么全部挂起 —— **其他客户端要么全部看到移动，要么看不到，绝不会只移动一半**。

## 五、操作压缩（需求 6）

连续移动/缩放的高频帧都带 `squashKey`（精确到「手势 × 对象」），
`WB.squash(log)` 把相同 key 的一串信封折叠为携带最终状态的一条。
服务端超过阈值自动压缩，并提供 `POST /api/compact?roomId=`；
超出硬上限时建立快照水位（snapshot watermark），日志体积有界，晚加入者走快照 + 增量。

## 六、压感笔迹（需求 7，验收场景 4）

采样点升级为 `{x, y, p 压感, tx/ty 倾斜, t 时间戳, w 宽度}`。
宽度由两端共享的同一确定性公式计算，发送端预算好 `w` 随点传输：

```
w(p) = base · (kP + (1-kP)·pressure) · 1/(1 + kS·v/vRef)
        └── 压感变宽 ──┘                └── 速度变细 ──┘
```

接收端无需重放，逐点宽度两端逐值一致。

## 七、平滑与简化（需求 8）

- 发送前用 **Ramer–Douglas–Peucker（RDP）** 简化点集（保留压感等属性）；
- 渲染支持 **向心 Catmull-Rom**（默认，已验证均匀情形退化为标准 Bezier 系数）、
  **三次均匀 B 样条**、线性、以及 v1 的中点二次贝塞尔。

## 八、橡皮擦（需求 9，验收场景 5）

- **像素擦**：笔迹被划成 `16×16` 个单元的块（块边长 = `cellSize × 16`，cellSize 取笔迹宽度）。
  只同步被触碰的块 + 块内单元下标（`erase {chunks:[{oid,tx,ty,cells:[[cx,cy],…]}]}`），
  增量同步、增量重绘，不全量重画；擦除单元在 CRDT 里是 LWW 寄存器，撤销可恢复，
  且撤销的架空检测精确到**单元**：同块内他人的擦除不阻塞自己对其余格子的恢复。
- **对象擦**：沿擦除路径命中整个对象 → `delete`（可多对象事务）。
- **整笔擦**：命中笔迹 → `delete`。

## 九、笔刷与识别（需求 10）

- 笔刷：钢笔、**荧光笔**（半透明 multiply）、**虚线**（setLineDash）、**纹理笔**（沿线盖点）；
- 工具：箭头、矩形/椭圆/三角/直线（一笔**图形识别**或直接插入）；
- **手写转文字**：内置 $1 Unistroke 识别器（重采样 64 点 → 旋转归一 → 缩放 → 黄金角搜索），
  内置数字与常用符号模板，命中阈值后笔迹转 `text` 对象（原笔迹不入库，无重复）。

## 消息协议（JSON over WebSocket，`/ws`）

```jsonc
// C → S
{ "type": "join", "roomId": "r1", "userId": "u-a", "lastSeq": 0 }
{ "type": "ops",  "envelopes": [ /* 单条或同一 txnId 的事务原子组 */ ] }
{ "type": "ping" }

// 信封
{ "id": "u-a:7", "clientId": "u-a", "lamport": 7,
  "clock": { "u-a": 7, "u-b": 3 },
  "txnId": "txn-…", "squashKey": "move:<gesture>:<oid>",
  "op": { "kind": "set", "oid": "obj-1", "fields": {"x": 100}, "prev": {"x": 80} } }

// S → C
{ "type": "joined",  "roomId": "r1", "userId": "u-a", "lastSeq": 16 }
{ "type": "snapshot","watermark": 0, "snapshot": { /* 折叠后的对象/擦除/组 + known VC */ },
                      "envelopes": [ /* 水位之后仍在日志的增量 */ ] }
{ "type": "ops",     "envelopes": [ /* 因果广播（不含发送者自己） */ ] }
{ "type": "ack",     "ids": ["u-a:7"], "lastSeq": 16 }
{ "type": "pong" | "error" }
```

HTTP：`GET /api/rooms`、`GET /api/room?roomId=`、`GET /api/compact?roomId=`（运维/测试压缩）。

## 验收场景 ↔ 测试

| 验收场景 | 测试 |
| --- | --- |
| 1. 三客户端同画并发，最终一致、无重复 | `test-smoke.js [1]`、`test-kernel.js [1]`（6 种乱序全收敛） |
| 2. A 撤销旧笔迹不破坏 B 的后续修改 | `test-smoke.js [2]`、`test-kernel.js [2]/[2b]` |
| 3. 多对象移动原子（全有或全无） | `test-smoke.js [3]`、`test-kernel.js [3]`（事务缺半时 0/5 可见） |
| 4. 压感笔迹两端宽度一致 | `test-smoke.js [4]`、`test-kernel.js [4]` |
| 5. 橡皮分块两端一致 | `test-smoke.js [5]`、`test-kernel.js [8b]` |
| 5b. 同块并发擦除下的选择性撤销（单元级粒度） | `test-smoke.js [5b]`、`test-kernel.js [8c]/[8d]` |

## 设计要点

- **为什么 seq 不再解决冲突**：seq 是单点全序，无法表达「B、C 都基于 A 的状态并发修改」这种偏序；
  CRDT 让每个副本本地确定性收敛，服务端只负责定序、广播、物化快照。
- **为什么撤销用逆操作而不是删日志**：协作系统不能改写他人已收到的历史；逆操作是一条普通新操作，
  同样参与因果/广播/压缩，架空检测保证「选择性」——只在不与他人意图冲突时生效。
- **为什么变换用独立 `tr` 而不是改 x/y**：笔迹是点云没有包围盒基准，统一仿射字段让移动/缩放/旋转
  对所有类型语义一致，命中测试用逆变换把屏幕点映回对象局部坐标。
- **乐观预提交 + 幂等重发**：本地操作立即上屏，未 ack 的断线期间积压，重连后按原 `id` 重发，
  服务端 `applied` 集合幂等去重，绝不重复入库。

> 说明：白板内容保存在服务端内存中，进程重启后清空（重连客户端以服务端快照为准）；
> 持久化可在 `Doc.snapshot()` 之上接入 Redis/数据库。
