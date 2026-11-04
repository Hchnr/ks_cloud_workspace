# 动态任务池与 SwanLab

## 在新环境部署

先按 [环境准备](env/README.md) 配置节点访问和 KS3。复制 [examples/dynamic-config.json](../examples/dynamic-config.json) 为私有配置并修改所有绝对路径、节点别名、内网地址和源目标映射。`hosts[0]` 是协调器节点；`slots` 是每节点活动分片上限，`jobs` 是每分片文件并发，`parallel` 是每文件分片并发。示例只列两个节点，实际使用时填入全部节点。

`run_dir` 必须是所有节点同路径可读写的共享目录。`local_root`、`queue_db` 必须在节点本地盘；每个运行使用独立目录。源数据、清单、`config_file` 和 `tool` 必须在执行节点上按配置路径可读。协调器 HTTP 端口仅在受信任节点内网开放。

在控制机准备共享运行目录和令牌（首次运行执行一次，不要覆盖续跑令牌）：

```bash
mkdir -p /shared/ks-cloud/runs/my-run
python3 -c 'import os,secrets; p="/shared/ks-cloud/runs/my-run/token"; f=os.open(p,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600); os.write(f,secrets.token_hex(32).encode()); os.close(f)'
```

将私有配置存为 `/shared/ks-cloud/runs/my-run/config.json`。在每个节点创建本地代码目录，将本仓库 `src/ks_cloud/` 的 Python 源文件复制到 `/var/tmp/ks-cloud/my-run/code/ks_cloud/`，避免运行期间从共享盘导入。代码和配置应在全部节点准备一致后再启动。以下命令在各节点执行，`--host` 替换为该节点在 `hosts` 中的别名：

```bash
export PYTHONPATH=/var/tmp/ks-cloud/my-run/code
export PYTHONDONTWRITEBYTECODE=1
nohup python3 -S -B -m ks_cloud.supervisor \
  --config /shared/ks-cloud/runs/my-run/config.json --host cpu-master-0 \
  > /var/tmp/ks-cloud/my-run/supervisor.log 2>&1 < /dev/null &
```

检查共享目录 `supervisors/*.json` 中的节点记录；确认准备完成后创建共享 `run_dir/START`，释放首次启动闸门。此处提供的是手动部署流程，`prepare` / `launch` 不负责部署动态任务池。后续任务分配、掉线回收和进程重启由调度器与 supervisor 自动处理。

SwanLab 使用独立 Python 环境，在 `swanlab_python` 指定其解释器，`swanlab_key_file` 指向私有 key 文件；也支持从旁路进程环境 `SWANLAB_API_KEY` 读取。`swanlab_proxy_file` 可指定代理文件，空字符串表示清除该旁路的代理环境变量。省略这两个文件配置时兼容当前环境的 `.secrets` 路径；可移植部署应显式配置。设 `swanlab_enabled=false` 时无需安装 SwanLab。默认每 120 秒汇总上报，失败自动退避。

可选 `overview_inventory_file` 指向历史参考清点 JSON，格式为 `{"sources":{"dataset":{"source":"/shared/dataset","files":1000,"bytes":1073741824,"complete":true}}}`，键对应任务 `id`，且 `source` 必须与配置完全一致。缺少有效参考量且尚未扫描结束时，总览显示总量未知，不制造全量百分比。

```bash
./bin/ks-cloud monitor-dynamic --run-dir /shared/ks-cloud/runs/my-run --once
```

## 当前运行与调度规则

`ks_cloud.dynamic` 是原固定分片 worker 之外的独立执行器。单个协调器在 SQLite 中串行提交领取、心跳和结果；各节点使用 HTTP 拉取任务，不在共享盘上竞争移动队列文件。HTTP 仅供节点内网使用，令牌放在运行目录下权限为 600 的 `token` 文件中。2026-11-03 修复了共享盘 SQLite 同步写入造成的 RPC 超时：当前运行将事务数据库放在主节点本地盘，独立线程每轮备份完成后间隔 30 秒向共享盘保存一致快照；网络盘复制不持有 RPC 锁。

2026-11-02 的运行目录为 `runs/dynamic_20261102_16cpu/`，包含已确认的 9 组源与目标映射。目标沿用 `ks3://baai-eai/backup_20261023/` 下原完整路径。原批次已停止，旧结果保留；新运行通过 `--update --update-rule 4` 检查并补传。不会删除目标已有数据。

扫描与上传同时进行，每片最多 1,000 个文件或约 2 GiB，超限单文件独立成片。清单按内容寻址；扫描器重启后以最后一份已提交清单的末文件为游标，按“当前目录文件优先、子目录按名称深度优先”的稳定顺序续扫，跳过已提交前缀，排除审计追加保留。待领或等待重试的清单也属于已提交工作，继续由队列调度。已完成扫描不会重扫。每组源最多预取 256 片，避免清单无限增长。源目录在本轮扫描、上传期间必须保持稳定；游标之前新加的文件不会在本次续扫中发现，完整新一轮检查应使用新运行目录。这是对运行期间发现文件的备份，不提供文件系统快照一致性。

2026-11-03 修复了协调器恢复时从头遍历数百万已处理文件、导致队列排空后长时间无上传的问题。扫描祖先目录时直接定位游标分支，后续兄弟目录延迟分类；普通目录边分类边生成文件分片，避免等整个大目录分类结束后才发任务。`status.json` 的 `scanner_activity` 记录当前操作、路径、更新时间及续扫游标；`waiting_for_scan` 表示队列排空等待扫描供给，`upload_idle_seconds` 表示距最近成功分片的秒数，`stalled_scanners` 标记超过默认 180 秒不更新的扫描操作。SwanLab 总览同步对应指标和等待提示，不能仅用节点心跳判断上传健康。协调器注册了 `SIGUSR1` 线程栈诊断信号；只对确认运行本版本的协调器使用，日志写入节点本地 `coordinator.log`。

16 个 CPU 节点每节点 2 个活动槽位，默认每片 16 个文件并发、每文件 2 个分片并发；大文件单独成片时使用 8 个分片并发。各源轮流发任务，同一源优先处理较大分片；快节点会自然领取更多分片。均衡的是可领取任务和活动槽位，文件大小、源盘和网络差异仍会造成瞬时吞吐不同，最后不可拆的单个文件也可能形成尾部任务。

心跳间隔 15 秒，worker 与协调器断连超过 90 秒会终止上传进程组；协调器在 240 秒后回收租约。上传子进程设置 Linux parent-death signal，worker 意外退出也会终止上传。租约 token 拒绝过期回报，领取请求带持久到本次重试结束的 request ID，回复丢失时重试领取同一个分片；服务器另行限制每节点最多 2 个未结束租约。未收到过 worker 心跳确认的过期领取不消耗上传重试预算。完成确认可以幂等重试。失败采用指数退避，最多 5 次整片尝试，工具内部另有请求重试。30 分钟没有文件数或字节进展会终止并重试该片。最终失败保留在队列和节点输出中，状态标为 `finished_with_issues`，不会当作成功。

扫描会记录失效链接、特殊文件、无法访问的路径以及未跟随的目录软链接，保存到 `excluded-*.jsonl`。Eidon 主任务排除 `hand_pose` 子目录，该目录单独上传。只将工具退出成功、失败数为零、处理文件数与清单完全一致的分片标记为完成。空目录、Unix 权限、软链接本身不在备份范围内；已有对象增量跳过不是重新计算内容哈希。

本环境原 HTTP 代理路径的大文件测试失败；使用 `--enable-http2` 启用 HTTPS 后，256 MiB 分片上传成功，并回读确认大小及 CRC64。运行保留这一参数，未修改全局凭据配置。

## 监控

```bash
./monitor --once
./monitor
```

`runs/active.json` 指向当前动态运行，原 `KS_CLOUD_PLAN` 环境变量仍可显式选择历史固定分片计划。运行目录的 `DASHBOARD.md` 与 `status.json` 每 30 秒覆盖刷新。

SwanLab 使用单个私有实验，每 120 秒批量上报总进度、确认上传量、跳过量、节点活动槽位、系统负载、内存和网络速率，失败后退避至最多 30 分钟。`progress/completed_*` 包含增量跳过；`uploaded_*_confirmed` 仅累计完整成功分片的上传统计，工具字节数有显示精度限制。扫描结束前，`coverage_of_discovered_pct` 仅是已发现范围的覆盖率，不代表全量完成率。网络接口速率包含节点网络命名空间中的其他任务流量。SwanLab 故障不会中断上传。

`swanlab.json` 保存实验链接、ID、最近入队时间及低频服务器回读；入队时间与云端确认分别记录。Key 和代理沿用线上实验的私有文件，只在旁路进程内加载。

SwanLab 新增 `00-总进度` 分组（指标前缀 `overview/`）：`dashboard` 是 HTML 总进度看板，`metric_guide` 是所有图表的逐项说明。总览同时显示数据量覆盖率、文件数覆盖率、实际上传和跳过量、9 个目录状态以及节点内存/OOM。扫描未结束时，未完成目录使用 `inventory_reference.json` 中的历史清点参考量，分母至少为本轮已发现量，明确标记“估算”；完成扫描的目录采用本轮清单总量。全部目录扫描结束后 `overview/total_is_exact=1`，但排除项仍单独列出，不能据此认定无异常完成。历史参考不是文件系统快照，不能当作精确剩余量；扫描未结束时不提供全量 ETA。

总览 HTML 每 120 秒更新一份快照，请在媒体图表中查看最新 step；图表说明在旁路启动后上传一次。指标说明也保存在 [swanlab-charts.md](swanlab-charts.md)，运行目录保留最新 `overview.html` 和 `metric_guide.html`。HTML 展示失败不阻断原标量监控。新增看板只需更新旁路，不需要重启上传 worker 或协调器。

总进度曲线使用默认 Step 横轴。2026-11-03 修复了显式 `_relative_time` 被云端创建为错误横轴类型、导致网页整组“暂无数据”的问题；历史指标一直存在，已原位修正已有 18 张曲线的显示配置。总进度组置于默认视图首位，前六张依次为数据覆盖率、完成量、剩余量、文件覆盖率、实际上传量和跳过量。云端 summary 有最新值只能证明数据入库，不能证明网页图表可用；验证需同时检查图表横轴与网页取数接口。已有图表不能靠重新调用 `define_metric` 修改，须更新原图表显示配置，保留历史数据。

容器实际限额是每节点 4 核、16 GiB。2026-11-03 起，资源图表改读 cgroup v2：`cpus` 是 CPU 配额，`memory_limit_GiB` 是容器内存上限，`memory_current_GiB` 包含文件缓存，`memory_anon_GiB` 是匿名内存，`memory_file_cache_GiB` 是文件缓存。`memory_used_pct` 现在表示匿名内存占容器上限的比例，旧数据使用宿主机口径，两段曲线不能直接比较。`oom_events`、`oom_kill_events` 是容器生命周期累计计数；`cpu_used_cores` 由容器 CPU 时间差分计算。`load1` 仍是宿主机负载，不能按 4 核容器负载解读。活动槽位按 worker 最近心跳确认的任务统计，未确认或等待回收的租约仅计入 `cluster/leased_chunks`。

## 进程与恢复

每个节点的 `ks_cloud.supervisor` 在本地目录 `/var/tmp/ks-cloud/dynamic_20261102_16cpu/` 持锁并监督 worker。`cpu-master-0` 额外监督协调器和 SwanLab。Python 代码复制到节点本地，禁用字节码写入，避免并行导入共享盘缓存时发生阻塞。SSH 或聊天断开不影响这些后台进程。服务退出后自动重启；节点失联后的上传分片由其他在线节点领取。

清单和队列快照保存在共享运行目录，当前事务数据库位于主节点 `/var/tmp/ks-cloud/dynamic_20261102_16cpu/queue.sqlite`；`queue_mirror.json` 记录最近快照时间和字节数。同一时刻只允许一个协调器。进程重启保留本地数据库；本地盘丢失时从共享快照恢复，快照之后的领取或完成确认可能重做增量检查，最近未入快照的扫描清单由重新扫描恢复。备份耗时会增加快照间隔，不能把 30 秒当作严格的数据恢复窗口。已增加[远端自动恢复](automatic-recovery.md)：第一台 CPU 节点为首选协调器，另外两台为备用，取得共享锁后可自动接管；16 个远端 guardian 及候选节点间的 SSH 巡检负责恢复服务树。被销毁节点重新具备 SSH、共享挂载和可信身份后可自动 bootstrap；本仓库不向计算平台申请或重建容器。

运行目录中的 `START` 用于首次启动闸门。创建 `STOP` 会让 worker 停止并保留状态；移除后 supervisor 自动重启 worker。若扫描线程已经因 STOP 中断，需要同时重启协调器服务，使其从持久清单恢复扫描。不要删除 `queue.sqlite`、清单或 token。`supervisors/*.json` 记录各节点 supervisor 与服务 PID；节点本地保留命令、stdout、工具失败清单和 checkpoint。协调器单写数据库，禁止另起进程直接修改它。

测试：`python3 -m unittest discover -s tests -v`，涵盖并发领取、租约过期与拒绝旧结果、失败退避上限、扫描重启去重以及真实 HTTP 协调器与双 worker 的端到端执行。

元数据扫描支持[有界并发、在线调参和自动回退](adaptive-scanning.md)，首次部署后可通过 `tune-scan` 切换扫描窗口，不需要反复重启上传任务。
