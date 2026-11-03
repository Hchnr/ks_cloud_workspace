# 远端自动恢复

生产首选主节点为第一台 CPU 节点 `cpu-master-0`，备用为 `cpu-worker-0`、`cpu-worker-1`。上传、调度、监控和恢复均在远端运行，不依赖发起任务的控制容器。

16 个节点各有 recovery guardian 监督本机 supervisor；三个候选节点每 60 秒通过已验证身份的 SSH 巡检全部节点，并执行幂等 bootstrap。bootstrap 使用本地锁，重复执行不会启动多份 guardian。私钥只引用已有私有文件，不写入配置、日志或 Git。

## 故障处理

| 故障 | 自动行为 |
| --- | --- |
| 本机断线、聊天结束、控制容器退出 | 远端进程继续运行 |
| worker、协调器、SwanLab 退出 | supervisor 重启对应服务 |
| supervisor 退出或 90 秒无本地心跳 | 本机 guardian 重启服务树 |
| guardian 退出或失去心跳 | 其他候选节点经 SSH bootstrap 恢复 |
| 主节点失联 | 备用节点取得共享锁，恢复队列并发布新地址 |
| 原主恢复 | 留作备用，不抢占健康主节点 |
| 共享盘不可用 | 不强抢锁；主心跳过期后拒绝 RPC，保留状态等待恢复 |

各级服务通过 Linux parent-death guard 关联：guardian → supervisor → worker/协调器/监控 → 上传子进程。父进程被杀后旧子进程随之终止，避免遗留上传进程。

## 防双主与队列恢复

跨节点 NFS 排他锁 `coordinator.lock` 是唯一主节点的依据，心跳超时仅触发尝试接管，不能绕过锁。部署前已跨 CPU 节点验证锁互斥。默认主心跳 10 秒、有效期 45 秒；主节点无法成功续写共享心跳时，RPC 在有效期后拒绝请求。supervisor 每 15 秒检查本机协调器 RPC，连续三次失败会重启它。

任期归属追加记录在被锁定的同一文件 inode 内，获得锁后从该文件读取上一任主节点，避免 NFS 对原子替换的 `leader.json` 保留旧目录缓存、导致误用残留本地数据库。追加记录使用 fsync，忽略崩溃留下的半条尾记录；绝不能删除或替换锁文件。

同节点进程重启继续使用本地事务数据库。跨节点接管必须从共享快照恢复，并覆盖备用节点可能过期的本地数据库；缺少快照则拒绝接管。默认隔离新任务领取 110 秒，期间继续接受心跳和结果，给旧上传断连停止及租约验证留出时间。worker 每约 5 秒刷新地址，RPC 失败时重新发现；原有 240 秒租约回收、token 校验与扫描游标继续生效。

共享快照默认每轮备份完成后间隔 30 秒更新。恢复可能重做最近未入快照的增量检查；云端已上传对象不会删除。跨节点恢复的进度数可能暂时回退到快照位置，然后通过增量检查补齐。NFS 属性缓存、备份耗时和网络故障会延长切换，默认间隔不是严格 RTO/RPO。

## 部署与维护

参考 [HA 配置模板](../examples/ha-config.json)。配置候选节点、主节点地址、只读代码版本目录、SSH 目标、身份文件和 known_hosts。代码版本发布后保持不变，bootstrap 将其复制到节点本地；全部节点使用同一配置。使用集群 DNS 地址可避免固定到控制容器或单一 IP。SwanLab 只在持有协调器角色的节点运行，沿用原 run ID，仍每 120 秒上报。

通过 SSH 在远端运行 bootstrap，不能在控制机直接启动 guardian：

```bash
ssh cpu-master-0 python3 -S -B /shared/run/release/ks_cloud/bootstrap.py \
  --config /shared/run/config.ha.json --host cpu-master-0
```

bootstrap 可从共享版本目录补齐丢失的本地代码。共享 `recovery/<host>.json`、`supervisors/<host>.json`、`leader.json`、`queue_mirror.json` 分别记录守护、服务、主节点和快照状态。SwanLab 的 `recovery/*` 指标记录主节点索引、任期、守护在线数、心跳与快照年龄、接管隔离窗口。

维护时先在共享 `recovery-hold/<host>` 创建文件，再停止其 guardian；其他节点不会重启被 hold 的节点。移除文件后远端巡检自动补回。hold 不自动停止已运行进程。不要删除主锁、清单、队列快照或 token。

## 验证与边界

测试覆盖过期本地数据库替换、主心跳失效、地址发现，以及真实进程树中的主 guardian 被杀后备用接管、supervisor 被杀后自动恢复。两个真实 CPU 节点的隔离演练使用假上传工具，验证主角色切换、原主 guardian 自动补回、恢复后不抢主；不写生产对象。

此机制依赖仍有候选 CPU 节点和共享盘可用，不负责向计算平台申请或重建容器。被销毁节点恢复 SSH、共享挂载及可信身份后可由 bootstrap 补回。SSH 身份改变时需通过可信渠道更新 known_hosts，不跳过身份校验。所有候选节点同时消失或共享盘长期不可用时，需要计算平台或存储服务恢复。
