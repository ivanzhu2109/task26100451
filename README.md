# EdgeSync 本地多目标复制内核

一个主节点（master）+ 两个目标节点（A、B）的本地复制库。文件内容实际写入
`root/` 下的 `master/`、`node_a/`、`node_b/` 三个目录；元数据保存在进程内。
只有主节点接受修改，不处理节点独立编辑或冲突合并。

## 使用

```python
import io
from edgesync import EdgeSync

sync = EdgeSync("/tmp/store")

v1 = sync.put("docs/a.txt", io.BytesIO(b"hello"))   # 流式写入主节点，返回新版本号
sync.run_one("A")                                    # 处理 A 的一项待办（真实复制文件）
sync.run_one("B")

r = sync.read("docs/a.txt", "A")
print(r.exists, r.source_version, r.replica_version, r.status)  # True 1 1 synced
data = r.stream.read()                               # 稳定快照；用毕必须 close
r.stream.close()

sync.delete("docs/a.txt")                            # 逻辑删除，副本立即不可新读
```

- `put(path, stream) -> int`：流式写入（1 MiB 分块），提交后产生新版本并安排向
  A、B 复制。版本号单调递增，删除重建后也不复用。
- `delete(path)`：逻辑删除；已发布副本立即对新读不可见，该路径未完成的复制任务失效。
- `run_one(target) -> bool`：领取并处理 `target`（`"A"` 或 `"B"`）的一项待办；
  可多线程并发调用（含同一目标）；过期任务自动跳过。队列空时返回 `False`。
- `read(path, node) -> ReadResult`：`node` 为 `"master"`/`"A"`/`"B"`。返回当前源版本、
  该节点已发布副本版本与读取流、整体状态（`"synced"`/`"pending"`）。节点尚无副本时
  `stream` 为 `None`；路径不存在时 `exists=False`。只有 A、B 均已发布当前源版本时
  状态才是 `"synced"`。

## 并发语义

- 单一元数据锁；`put`、`delete`、复制结果发布各有唯一原子生效点。
- 输入流读取、文件复制、内容写出全部在锁外进行：某次复制被暂停时，同一路径的
  `put`/`delete` 与其他路径的所有操作都能正常完成。
- `read` 不等待复制队列；其元数据与流在同一锁临界区内取得，对应同一逻辑时刻。
- 发布前在锁内重新校验“任务版本仍是当前源版本且未删除”：晚完成的旧任务会被丢弃，
  不会回退新副本、复活已删路径或污染删除后重建的路径。
- 已发布副本允许暂时落后；`read` 返回的流是稳定快照，后续覆盖/删除不改变其内容，
  `delete` 前已取得的流可以读完。

## 资源释放

每个落盘文件按引用计数管理：打开的读取流与排队/在途的复制任务各持一个引用。
文件一旦被取代或删除（retired）且引用归零即被 unlink。因此当待办处理完、
在途操作结束、读取流关闭后，不会遗留无引用的历史文件或临时文件——调用方只需
关闭 `read` 返回的流。复制缓冲固定 1 MiB，支持 500 MiB 级对象，单次传输的额外
内容缓冲远低于 8 MiB 上限（有 tracemalloc 测试守护）。

## 运行测试

```bash
python3 -m pytest tests/ -q
```
