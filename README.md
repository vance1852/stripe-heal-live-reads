# 分条影像库的降级读取与在线修复

存储联调使用合成设备、时间轴和原始字节。设备记录由各客户端及介质的输入组成，每份记录可独立使用。

## 业务协议

影像的逻辑版本不可变。每个条带依次含两个原文片 `d0`、`d1` 和一个等长异或校验片 `p`，原文字节通过依次连接这两个原文片得到。末尾逻辑长度可能截去填充。仓库的 `stripes` 给各版本的逻辑位置、片校验及最初设备，盒内材料的异常字节不改变原期望校验。`domains` 的值相同代表设备不能作为独立故障域。新承诺需让任意单个故障域退出后仍可恢复；存量降级可读与达到该承诺分开报告。
设备可供 `GET /media/{device}/fragments/{id}` 实取片数据，设备协议返回 JSON，其中 `file` 指向可取的原始片。替片写入用同一路径的 `PUT`，正文是原始字节，设备确认可给出保存后的摘要及 `generation`。回包的代次和来源报错均是设备事实。

## 本地设备资料

运行环境为 Python 3.11.2，使用标准库。设备资料端默认仅在 127.0.0.1:8090 运行，使用 --port 可另设端口。

~~~sh
python3 device_fixture.py fixtures/repair-target-conflicts-with-fault-domain
~~~

GET /fixture/events 返回设备参数、字节目录和按到达顺序记录的输入。同一时刻按文件先后排列。GET /fixture/objects/文件名 取原始字节。GET /fixture/journal 查看资料端已收到的设备请求，每个请求记录实际正文长度和摘要。

events.json 中的 packets 是可重放的设备响应。资料端按实际 HTTP 方法、路径和完整原始请求正文匹配第一条尚未使用的记录。没有匹配时返回 404。lag_ms 是回包延迟，drop 代表设备收到请求后直接断开连接，接收方需要以其他报告确认结果。没有 HTTP 交互的设备和客户端动作直接取 events。重新启动资料端会回到记录开头。

在项目根目录运行 python3 -m unittest -v 检查事件、对应字节、设备响应及接收日志。

## 归档对象存储服务

本仓库同时提供一个仅依赖标准库的本地对象存储实现：

- `archive_store/core.py`：条带校验、XOR 降级读、版本/读句柄快照、故障域、修复代次、单飞合并与空间回收。
- `archive_store/devices.py`：设备 HTTP 协议客户端、进程内设备簇和故障注入。
- `archive_store/scenario.py`：按 `events.json` 时间轴重放。
- `archive_store/server.py`：本地 HTTP/JSON 服务。

### 常用 Python API

```python
from archive_store import ObjectStore

store = ObjectStore.from_fixture("fixtures/repair-target-conflicts-with-fault-domain")
store.report_offline("A")
store.open_read("v1", handle_id="h1")

report = store.read_range(handle="h1", offset=4, length=26)
report.status               # direct | degraded | unavailable
report.direct_ranges        # [(8, 16), (24, 30)]
report.degraded_ranges      # [(4, 8), ...]
report.unavailable_ranges   # 无法恢复、不能补零的半开区间
report.to_bytes()           # 有缺块时抛 UnavailableBytesError，并携带 report

store.start_repair("v1", "s1", "d0", target="E", attempt="e1", write=False)
store.report_repair_receipt(device="E", attempt="e1", generation=2, file="one.bin")

store.rebuildable_areas()
store.fault_domain_query()
store.reclaimable_space()
store.release_reclaimable()
```

所有取出的片都必须匹配策略三元组中的 SHA-256；重建片也会先与期望片摘要核对。修复只使用同一 `(image, version, stripe)` 的另外两个片，不会借用同名影像的其他版本。

### HTTP 服务

```sh
python3 -m archive_store serve --port 8080 \
  --fixture fixtures/repair-target-conflicts-with-fault-domain
```

常用接口：

- `GET /v1/versions/{version}/read?offset=0&length=...`：返回分类区间和 Base64 字节。
- `GET /v1/versions/{version}/content`：严格下载；有不可恢复区间时返回 `409` 和区间报告。
- `POST /v1/handles` / `POST /v1/handles/{id}/close`：固定版本与放置代次的读快照。
- `POST /v1/repairs`、`POST /v1/repairs/receipt`：修复与设备回单。
- `GET /v1/queries/rebuildable`、`/v1/queries/fault-domains`、`/v1/queries/reclaimable`。
- `GET /v1/capacity`、`/v1/durability?version=...`。

设备协议仍访问 `GET|PUT /media/{device}/fragments/{stripe-role}`；`GET` 回包中的 `file` 会再从 `/fixture/objects/{file}` 取原始字节。

### 事件重放

```sh
python3 -m archive_store replay fixtures/repair-target-conflicts-with-fault-domain --pretty
```

重放结果包含每次读观察、可重建区域、故障域建议、可回收空间和版本承诺状态。根目录的 `storage.py`、`object_store.py`、`store.py`、`solution.py`、`striped_store.py` 等模块名仅作为常见导入名兼容层。
