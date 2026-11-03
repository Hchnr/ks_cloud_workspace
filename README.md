# KS Cloud Workspace

向金山云 KS3 增量上传、补传及复查大型数据集，保留配置中的源目录到对象前缀映射。支持多节点动态任务池、原固定分片流程，以及低频 SwanLab 进度和资源监控。

动态任务池边扫描边上传，各节点自动领取小分片；租约、心跳和幂等结果确认支持失败重试及掉线后的任务回收。SwanLab 总览同时展示数据量、文件数、实际上传与跳过量、目录状态和节点资源。扫描未结束时明确区分历史参考估算与本轮已发现量。

## 选择入口

| 用途 | 入口与文档 |
| --- | --- |
| 多节点动态均衡上传 | [动态部署、监控与恢复](docs/dynamic-upload.md)，[配置模板](examples/dynamic-config.json) |
| 查看当前动态运行 | `./monitor --once`；显式入口 `./bin/ks-cloud monitor-dynamic --run-dir /absolute/run --once` |
| 控制容器断开、主节点失联后的自动恢复 | [远端守护、备用接管与防双主](docs/automatic-recovery.md)，[HA 配置模板](examples/ha-config.json) |
| 理解 SwanLab 图表 | [全部指标及统计口径](docs/swanlab-charts.md) |
| 预先生成固定分片计划 | 下方流程，[配置模板](examples/config.json)，[恢复说明](docs/operations.md) |
| 单节点批量恢复、磁盘与 KS3 诊断 | [辅助工具](docs/tools.md) |
| 准备 SSH、KS3 凭据和网络 | [环境准备](docs/env/README.md) |

核心执行器依赖 Python 3.10+ 标准库、Linux、OpenSSH 和 `ks3util`。SwanLab 旁路需单独安装 SDK（当前验证版本 0.10.3）；设 `swanlab_enabled=false` 可关闭。运行日志、数据库、凭据及本地环境资料不提交 Git，克隆仓库不会带入现有运行状态。

## 固定分片流程

```bash
cp examples/config.json config.local.json
# 编辑 source、destination、tool、config_file、hosts 和 remote_run_dir
./bin/ks-cloud prepare --config config.local.json --output runs/my-run
# 核对 runs/my-run/plan.json 中的源与目标后启动
./bin/ks-cloud launch --plan runs/my-run/plan.json
./bin/ks-cloud monitor --plan runs/my-run/plan.json --once
```

`prepare` 只扫描和生成计划，不上传。清单及源数据必须在所有节点上以相同绝对路径可读。默认每 5,000 个文件生成分片，固定归属到节点；文件数均衡不保证字节数或耗时均衡。每节点默认 `jobs=8`、`parallel=1`、`list_parallel=2`。

可用旧清单生成补传计划，避免重新遍历文件树：

```bash
./bin/ks-cloud prepare --config config.local.json \
  --legacy /absolute/path/to/old/distributed --output runs/recheck
```

旧清单仍需保留，导入不会保证发现新增文件。完整新一轮扫描应不带 `--legacy`。

## 统计与完成条件

上传使用 `--update --update-rule 4`，按目标存在性、大小和更新时间增量检查。跳过量包含在已处理进度中，实际上传量单独统计；已有对象跳过不是全量内容哈希校验。不会删除云端多余对象。

源目录在扫描和上传期间应保持稳定。空目录、Unix 权限和软链接本身不在备份范围内；失效链接、特殊文件及未跟随的目录链接记录为排除项。执行结束后应检查排除和失败清单，不能仅凭完成百分比认定所有源数据均已备份。

`./monitor` 优先读取 `runs/active.json` 指向的动态运行；设置 `KS_CLOUD_PLAN` 可选择固定分片计划，没有动态指针时回退到 `runs/current/plan.json`。这些指针及运行产物仅保留在本机。

## 开发与验证

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -B -m unittest discover -s tests -v
./bin/ks-cloud --help
./bin/ks-cloud monitor-dynamic --help
```

`src/ks_cloud/` 包含调度、执行、监控和辅助工具；`tests/` 覆盖租约、扫描去重、HTTP 协调器与 worker 协作、总览口径及监控入口。运行环境资料见 `docs/env/`。`runs/`、`logs/` 和节点本地工作目录属于运行产物，整理源码时不要清理正在使用的状态或 checkpoint。
