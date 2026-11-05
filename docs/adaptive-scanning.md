# 扫描在线调参与自动回退

元数据预取与上传并发独立。扫描器在一个有界线程池中读取目录项类型和文件大小，按原先排序、文件优先、深度优先的顺序提交清单。所有数据源合计最多 8 个同时进行的元数据检查，每源最多预取 8 个结果。分片 ID、目标路径、增量规则、数据库与断点格式保持一致。源目录在续跑期间仍须保持稳定。

协调器配置通过 `scan_tuning_file` 指向共享 JSON。独立线程默认每 5 秒读取，配置文件读取阻塞不会持有调度锁；读取失效或超过 120 秒未完成时，新工作自动使用串行窗口。已经发出的少量系统调用需要返回，不能承诺中断卡在内核中的 NFS 请求。

初始配置见 [scan-tuning.json](../examples/scan-tuning.json)。升级前的配置不含此字段时默认串行。首次启用需要部署代码并重启一次协调器；之后以下变更不需要重启协调器或上传 worker：

```bash
# 查看有效配置文件；运行时实际状态另见 status.json 的 scan_control
./bin/ks-cloud tune-scan --config /shared/run/config.json

# 自动调节：从每源 2 开始，单源最多 8；全局上限仍为 8
./bin/ks-cloud tune-scan --config /shared/run/config.json \
  --set enabled=true --set auto=true --set workers=2 --set max_workers=8

# 手动指定窗口；内存、I/O、吞吐与配置保护仍然生效
./bin/ks-cloud tune-scan --config /shared/run/config.json \
  --set auto=false --set workers=2

# 功能回退：停止并发预取，继续串行扫描和既有上传
./bin/ks-cloud tune-scan --config /shared/run/config.json --set enabled=false
```

命令校验所有字段，在同一个文件锁下合并变更并原子替换配置。非法设置不会覆盖原文件；直接手工写坏文件会触发串行保护。配置变更重新建立速率基线与冷却状态，建议将相关字段在一条命令中提交。`control_revision` 只统计当前协调器进程的有效变更，不能当作永久审计编号。

每 60 秒基于本地统计评估：待领取分片不足半个高水位、扫描仍有产出且冷却结束时，将预取窗口加倍至上限；队列达到高水位时降为 1，原有队列背压仍生效。默认每源最多积压 64 个待领取分片。队列中可能已有升级前生成的任务，调参不删除它们。

匿名内存达到容器限额的 65%、memory PSI `some avg10` 达到 25%、I/O PSI 达到 40% 或新出现 OOM Kill 时，退回串行并冷却 300 秒。高文件缓存占比本身不触发回退。元数据平均延迟超过 2000 ms、目录项排除率超过 2%、扫描速率低于本次探测基准的 75%，或存在待领取任务时实际完成文件速率下降至基准的 75% 以下，也触发保护。文件大小、目录结构和共享盘环境会改变吞吐，自动控制是保守探测，不能保证任何时刻都达到最高速度。

保护降到串行，不会自动还原代码，也不撤销已完成上传。持续压力会延长冷却；压力解除、冷却结束后重新探测。计数包括目录、链接和其他排除项，需结合排除审计判断原因。超时的 NFS 系统调用不通过无限增线程绕开。

SwanLab 沿用原实验，至少间隔 120 秒批量上报 `scanning/*`；失败延用原有指数退避。`status.json` 中的 `scan_control.sources` 保留回退原因和冷却状态，`scanning/<数据源>/files_s` 为扫描速度，`upload_files_s` 为整片成功确认的文件速度，包含增量跳过。图表定义见 [swanlab-charts.md](swanlab-charts.md)。上传 `jobs`、`slots`、代理和单文件分块并发不属于本接口，保持现有配置。
