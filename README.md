# Collaborative Document Gateway

服务端协作文档网关。多个客户端通过 WebSocket 交换 **Yjs** 更新，Fastify 承载
HTTP/WebSocket，PostgreSQL 保存只增的更新日志与压缩快照。不存在“最后一次保存
覆盖其他人修改”：所有写入都是带 Yjs 客户端时钟的 CRDT 结构，合并结果与到达顺序
无关，并且每个客户端、服务器内存态与“只从 PostgreSQL 重建”的状态用**二进制状态
哈希**断言一致，而不是只看渲染出的字符串。

无前端。`scripts/client-a.js` / `scripts/client-b.js` 是两个脚本客户端，用于
演示并发编辑、乱序、删除与重连。

---

## 1. 架构与持久化边界

```
 client A ─┐
 client B ─┼─ ws ──► Fastify (/ws) ──► per-doc Room（内存 Y.Doc, gc=false）
 client C ─┘                │                  │ 串行 async 队列
                            │                  ▼
                            │        1) 帧/二进制校验
                            │        2) 每次 UPDATE 重新查库鉴权
                            │        3) BEGIN; SELECT … FOR UPDATE;
                            │           seq = max(seq)+1
                            │           INSERT … ON CONFLICT(doc_id,client_msg_id) DO NOTHING
                            │           COMMIT   ◄── fsync 持久化边界
                            │        4) 内存 apply + 广播给“其他”连接
                            │        5) ack（含 seq / duplicated）
                            ▼
                    PostgreSQL
                    doc_updates      只增更新日志 (doc_id, seq) / (doc_id, client_msg_id)
                    doc_snapshots    压缩快照（through_seq + state + sha256）
                    update_errors    未知/损坏/越权帧的可定位错误
```

**确认（ack）即持久化边界**：客户端只有在收到 `ack {ok:true, seq}` 后才可以认为
消息落库。顺序固定为「数据库 COMMIT → 内存应用 → 广播 → ack」。若进程在 COMMIT
之后、ack 之前退出：

* 更新已在 PostgreSQL（崩溃恢复后重放得到）；
* 客户端用**同一个 `msgId`（= `client_msg_id`）**重发，命中唯一约束，返回
  `ack {ok:true, duplicated:true, seq:<原 seq>}`，不会二次写入或二次应用；
* 客户端本地 Y.Doc 已有该编辑，重连同一份更新是幂等合并。

这个边界不是纸面约定——见测试 T4（`CRASH_AFTER_COMMIT=1` 时进程在 COMMIT 后
直接 `exit(17)`）与 T10（正常 SIGTERM 重启后重连收敛）。

### 去重

`doc_updates (doc_id, client_msg_id)` 唯一。任何重连、重试、网络重复帧，只要
`client_msg_id` 相同就只落一行；ack 用 `duplicated:true` 明确告知客户端这是
去重命中而不是新写入（T3）。

### 乱序到达

服务器按**到达顺序**分配 `seq`，但 Yjs 结构体携带因果来源（client id + clock），
不是数组下标。更新以任意顺序应用都收敛到同一状态；冷启动客户端按 `seq` 升序
重放日志，结果逐字节相同（T2）。

### 断线按状态向量补齐

hello 和 `sync-req` 都接受 Yjs 状态向量：服务器返回
`encodeStateAsUpdate(doc, sv)`——只包含缺失结构体的差异，而不是强制全量。
当前 SV 的差异编码为空；把旧差异再应用一次是幂等的（T5）。

### 压缩后仍可恢复

`POST /v1/docs/:docId/compact` 在该文档的串行队列内执行：

1. 取上一份快照，折叠 `seq > through_seq` 的全部更新，得到新全量状态；
2. **校验 1（提交前）**：折叠结果必须与实时内存 Y.Doc 的全量编码逐字节相等
   （两条独立构造路径）；
3. 写入 `doc_snapshots(through_seq, state_bytes, state_hash)`，把被折叠的更新
   标记 `compressed_in`；可选 `deleteFolded:true` 物理删除旧更新；
4. **校验 2（提交后）**：只使用“最新快照 + 存活尾部”重新恢复一次，必须与实时
   文档逐字节相等——直接验证「压缩/删除日志后仍可恢复文档内容」。

恢复永远走 `snapshot(through_seq=N)` + `updates(seq > N)`（T6）。

### 未知 / 损坏更新可定位

无法解析的帧不会杀死进程，也不会污染文档，而是写入 `update_errors`：
`doc_id / user_id / tenant_id / client_msg_id / raw_len / raw_prefix_hex /
error_code / error_message`。区分 `BAD_JSON`、`BAD_ENVELOPE`、`BAD_ENCODING`、
`CORRUPT_UPDATE`、`FORBIDDEN`、`UNKNOWN_TYPE` 等（T7）。

---

## 2. 鉴权与多租户：不信任客户端自报房间号

* 连接的第一帧必须是 `hello {token, docId, sv?}`。`token` 解析为服务端用户，
  `docId` 必须与 `document_members` 中一条**未撤销**的成员关系匹配，且用户与
  文档属于同一租户；否则 `hello-err` 并关闭连接。
* hello 之后的帧不再携带房间号——连接已绑定到鉴权后的 room，客户端无法在帧里
  声称另一个房间。
* **每次更新**都重新查库校验 `(user, doc)` 成员关系与角色，不使用连接期缓存：
  会话中途被撤销的成员，下一条 update 立即 `FORBIDDEN`（T8）。
* `reader` 可连接/同步，但写帧返回 `READ_ONLY`。
* HTTP 管理端点（压缩、恢复探测）执行同一套租户/角色校验。

---

## 3. WebSocket 协议

JSON 文本帧；Yjs 二进制用 base64 字段传输。

| 方向 | type | 字段 |
|---|---|---|
| c→s | `hello` | `token`, `docId`, `sv?` |
| s→c | `hello-ok` | `docId`, `role`, `seq`, `sv`, `state`（相对 sv 的差异） |
| s→c | `hello-err` | `code`, `message`（随后关闭） |
| c→s | `update` | `msgId`, `update`, `svHash?` |
| s→c | `ack` | `msgId`, `ok`, `seq`, `duplicated?`；失败时带 `code/message` |
| s→c | `update` | `seq`, `msgId`, `origin`, `update`（只广播给**其他**连接） |
| c→s | `sync-req` | `sv` → `sync-diff {seq, update}` |
| c→s | `compact` | `deleteFolded?`, `minUpdates?` → `compact-result` |
| c→s | `ping` | → `pong` |

实现中修掉的一个真实收敛 bug：早期版本把 update 回环广播给了发起者，发起者
本地已应用过同一更新，重复应用导致它与其他副本发散。现在广播显式排除来源
socket（`room.broadcast(msg, ws)`）。该 bug 正是靠“比对二进制状态哈希而不是
比对字符串”才暴露的。

---

## 4. 本地运行（无 root 的用户态 PostgreSQL）

仓库假定一个 PostgreSQL 15。开发机无 root 时可直接用解压版二进制：

```bash
# 一次性：下载并解压 postgresql-15（Debian 12 示例）
mkdir -p /tmp/pgdeb /tmp/pglocal && cd /tmp/pgdeb
apt-get download postgresql-15 postgresql-client-15   # 或从 deb.debian.org 手动取包
dpkg-deb -x postgresql-15_*.deb       /tmp/pglocal
dpkg-deb -x postgresql-client-15_*.deb /tmp/pglocal

/tmp/pglocal/usr/lib/postgresql/15/bin/initdb -D /tmp/pgdata -U collab --auth=trust -E UTF8 --locale=C
printf "listen_addresses = ''\nunix_socket_directories = '/tmp'\nport = 55432\n" >> /tmp/pgdata/postgresql.conf
/tmp/pglocal/usr/lib/postgresql/15/bin/pg_ctl -D /tmp/pgdata -l /tmp/pg.log start
createdb -h /tmp -p 55432 -U collab collab
```

安装依赖、建表、种子数据、启动：

```bash
npm install
npm run db:schema
npm run seed:reset
npm start                 # ws://127.0.0.1:7777/ws
```

并发编辑演示（两个终端，或连续启动）：

```bash
npm run demo:a &
npm run demo:b
# 两个客户端会打印相同的 stateHash；可再用服务端恢复接口核对第三方哈希：
curl -s -H 'x-auth-token: user-owner' \
  http://127.0.0.1:7777/v1/docs/doc-demo/recovered-state
```

种子身份（demo 用，用户 id 即 bearer token）：

| 用户 | 租户 | 对 doc-demo 的角色 |
|---|---|---|
| user-owner | tenant-acme | owner |
| user-alice / user-bob | tenant-acme | writer |
| user-carol | tenant-acme | reader |
| user-dave | tenant-globex | 另一个租户（跨租户访问应被拒绝） |

---

## 5. 测试

```bash
npm test
```

测试在独立端口（8791）上自行 spawn/重启/崩溃网关进程，并断言**结构收敛**与
**持久化边界**：

| 用例 | 验证内容 |
|---|---|
| T1 | 两写者并发插入 + 删除共享区域，三方（A/B/PostgreSQL 重建）状态哈希一致；删除被复制，不发生覆盖 |
| T2 | 三条有因果依赖的更新**逆序**送达仍收敛；冷副本按 seq 重放结果相同 |
| T3 | 同一 `client_msg_id` 重发（含换一条新连接重发）只落一行，`duplicated:true` |
| T4 | **COMMIT 后、ack 前进程 `exit(17)`**：行已持久化；重启后内存房间从 PG 重建；同 id 重试去重；新编辑继续流转并收敛 |
| T5 | 断线期间他方持续编辑；重连带状态向量只补差异，当前 SV 差异为空，旧差异重复应用幂等 |
| T6 | 折叠压缩后 `快照+尾部 == 全量重放`；二次压缩并**物理删除**旧更新后，仅靠快照+尾部恢复仍逐字节一致 |
| T7 | 非 JSON / 未知帧类型 / 损坏二进制 / 空载荷全部被拒并写入 `update_errors`，合法更新随后仍正常 |
| T8 | 非成员、跨租户、未知 token、reader 写、会话中途撤销权限、HTTP 端点越权全部被拒 |
| T9 | 3 客户端 60 个最大并发的插入/删除，收敛到同一哈希；日志恰好 61 行，无丢失/重复 |
| T10 | 正常 SIGTERM 重启后，旧 SV 重连与冷副本全量加入都与重启前哈希一致，且不重复落库 |

---

## 6. 目录

```
db/schema.sql            表结构
src/config.js            环境配置
src/db.js                pg 连接池
src/permissions.js       token 解析 + 每次连接/更新的成员与租户校验
src/yutil.js             Yjs 文档/状态向量/差异/校验/恢复
src/room.js              每文档内存房间（串行队列 + 从快照+尾部加载）
src/update-service.js    鉴权→校验→去重持久化→应用→广播→ack（含崩溃注入开关）
src/compaction.js        压缩、双重一致性校验、存储恢复
src/errorlog.js          update_errors 落库
src/ws.js                WebSocket 协议
src/server.js            Fastify 入口 + 管理/恢复 HTTP 端点
scripts/lib-client.js    可控脚本客户端（手动 flush、乱序、重发、硬断线、带 SV 重连）
scripts/client-a.js      演示客户端 A
scripts/client-b.js      演示客户端 B
scripts/seed.js          demo 租户/用户/文档/成员
test/                    端到端收敛与持久化测试
```

## 7. 备注与边界

* 所有 Y.Doc 使用 `gc:false`，保证状态编码不依赖垃圾回收时机，压缩/恢复可
  逐字节比较。
* 演示用「用户 id 即 token」仅为本地便利；生产应替换为不透明令牌 + 服务端
  会话映射。
* 快照采取“标记 + 可选删除”策略；默认保留被折叠的更新以便审计，需要回收空间
  时传 `deleteFolded`，恢复路径同样被验证。
* 权限校验在更新路径上是每次查库，优先保证撤销即时生效；高吞吐场景可在其外
  增加短 TTL 缓存，但必须保留版本/撤销号失效机制。
