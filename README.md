# KS Cloud Workspace

用于向金山云 KS3 增量上传、补传及复查大型数据集。Python 标准库实现，通过 SSH 启动节点本地后台进程，支持多节点固定分片、断点续传和命令行监控。

## 目录

- `src/ks_cloud/`：计划生成、节点执行、进度解析和监控。
- `bin/ks-cloud`：无需安装 Python 包的入口。
- `examples/config.json`：不含凭据的配置模板。
- `docs/env/README.md`：新环境的依赖、SSH、KS3 和网络配置准备。
- `docs/operations.md`：恢复、失败处理和统计口径。
- `runs/`、`logs/`：本地运行产物，不提交 Git。

## 使用

先完成[环境准备](docs/env/README.md)，再创建私有配置：

```bash
cp examples/config.json config.local.json
# 编辑 source、destination、tool、config_file、hosts 和 remote_run_dir
./bin/ks-cloud prepare --config config.local.json --output runs/my-run
```

`prepare` 只扫描和生成计划，不上传。源目录必须保持稳定；生成的清单及源数据必须在所有节点上以相同绝对路径可读。新计划按每5000个文件生成分片，跳过目录软链接、失效链接及特殊文件，记录到 `excluded.jsonl`；不备份空目录、Unix 权限或软链接本身。

检查 `runs/my-run/plan.json` 中的源和目标后启动：

```bash
./bin/ks-cloud launch --plan runs/my-run/plan.json
./bin/ks-cloud monitor --plan runs/my-run/plan.json
# 单次查看
./bin/ks-cloud monitor --plan runs/my-run/plan.json --once
```

每个分片固定归属于一个节点，按文件数均衡分配，没有共享待领队列。文件数均衡不保证字节数或耗时均衡。每节点默认 `jobs=8`、`parallel=1`、`list_parallel=2`；24节点即192个文件任务槽位和48个扫描槽位。大文件分片并发为1。

上传使用 `--update --update-rule 4`，检查目标存在性、大小和更新时间，符合更新规则的跳过，其余补传。**这是增量查漏，不是全量内容哈希校验。** 不会删除云端多余对象。节点成功退出也不代表失效链接或清单外的新文件已经备份，需要查看排除和失败清单。

## 复用旧清单

可从本项目早期 `distributed` 运行目录导入全部分片，无需重新遍历文件树：

```bash
./bin/ks-cloud prepare --config config.local.json \
  --legacy /absolute/path/to/old/distributed --output runs/recheck
```

旧清单仍需保留。导入只复查旧清单覆盖的路径，不保证发现新增的顶层文件。完整新一轮扫描应使用不带 `--legacy` 的 `prepare`。

## 当前历史任务

2026-10-27 的24节点增量复查已结束，2975个分片共处理约2922万项。已知剩余异常是 `chartqa/train/images` 的失效软链接。本地历史状态保留在 `runs/` 和各节点 `/tmp/ks3_verify_20261027/`，均不属于可迁移代码。

本机已生成 `runs/current/plan.json` 作为历史监控入口，运行 `./monitor` 即可查看；新环境使用上面的显式 `--plan` 命令。历史完成状态不等于新增数据也已上传。

## 验证

```bash
python3 -m unittest discover -s tests -v
./bin/ks-cloud --help
```

仅依赖 Python 3.10+、OpenSSH、Git 和 `ks3util`，无第三方 Python 依赖。Linux 节点使用 `flock` 防止同一个运行目录重复执行。日志、断点和任务结果留在节点本地，SSH 断开不会终止上传；节点销毁或清理 `/tmp` 会丢失本地运行状态。
