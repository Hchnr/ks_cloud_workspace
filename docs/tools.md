# 辅助上传与诊断工具

多节点任务使用 [动态任务池](dynamic-upload.md)。以下模块保留给单节点恢复及独立性能诊断，均可用 `--help` 查看参数。

## 单节点批量恢复

`ks_cloud.batch` 按 `max_active` 限制同时运行的源目录任务，轮转 stdout 日志，并覆盖刷新 Markdown 看板。它不提供跨节点领取和租约，不应与动态任务池同时处理相同目标。

```bash
cp examples/batch-config.json batch.local.json
# 修改路径、凭据文件位置、源目标映射和并发
PYTHONPATH=src python3 -S -B -m ks_cloud.batch --config batch.local.json
```

`local_state_dir` 保存锁、checkpoint 和任务状态，续跑沿用同一目录；已经成功的任务会跳过。新一轮完整复查应使用新目录。`workspace` 是看板输出位置，`local_log_dir` 可将轮转日志放到节点本地。进程停止使用 SIGTERM，待上传子进程退出后再恢复。

## 磁盘与 KS3 探测

磁盘探测仅读取已有文件，使用 Linux `O_DIRECT` 绕过客户端页缓存；服务端缓存不受控制。`disk-throughput` 使用 1 MiB 顺序块，`disk-iops` 使用 4 KiB 随机块。每次运行使用新输出路径，支持 1–64 并发、1–60 秒采样：

```bash
PYTHONPATH=src python3 -S -B -m ks_cloud.benchmark \
  --mode disk-throughput --read-file /shared/data/large-file.bin \
  --concurrency 4 --seconds 20 --label disk-c4 \
  --output runs/probes/disk-c4.json
```

`ks3-throughput` 和 `ks3-iops` 会生成本地随机数据并写入 KS3，目标必须包含专用 `/_diagnostics/` 前缀。每次使用新的 `--label` 和输出路径，避免覆盖或复用旧对象：

```bash
PYTHONPATH=src python3 -S -B -m ks_cloud.benchmark \
  --mode ks3-throughput --tool /absolute/path/to/ks3util \
  --config-file /absolute/path/to/private/.ks3util.conf \
  --cloud-prefix ks3://your-bucket/_diagnostics/upload-probe \
  --concurrency 4 --objects 4 --object-mib 256 --cloud-timeout 90 \
  --label unique-trial-c4 --output runs/probes/unique-trial-c4.json
```

吞吐只累计成功清单中确认的完整对象，失败或未确认的字节不会记为成功吞吐。JSON 同时记录退出码、超时、失败对象数和日志位置；有失败时不能只比较速率。该独立探测器不继承动态任务池的 `enable_http2` 配置，应先确认其网络路径适用于当前环境。工具保留本地数据和云端诊断对象，完成分析后按该次输出记录的路径自行清理。
