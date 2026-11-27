# 多节点目录扫描

`prepared_datasets` 按实际目录层级拆分：协调器展开宽泛分组，再把可独立扫描的同级目录动态交给 CPU 节点；不依赖具体数据集的名称。`gpic_train_*.part*` 及 `images`、`assets`、`frames`、`videos` 下适合的媒体子树可整体预扫，其他目录按叶子目录拆分。`yuanhai/ego_r2_runs/20261024-ego-r1-1024-pipeline-v3/tasks` 中编号目录按最多 32 个一批分发。每个空闲、健康的节点从各来源的公共队列领取下一个目录或批次；完成快的节点自动领取更多工作。只有九个批准源目录中的文件和链接目标可进入上传清单，目录软链接仍写入排除清单并由后续轮次处理。

远端节点只返回文件路径、大小和排除项，不写队列、清单或云端。协调器按原深度优先顺序消费结果，仍按原规则持久提交清单和断点。普通目录最多预取两倍于节点上限、每目录最多返回 12,000 条；媒体子树最多预取节点上限个、每目录最多返回 64,000 条，同时限制单个目录的读取条目数。对需进一步拆分的目录，预取结果及时释放缓冲，让子目录继续被调度；超过记录上限时回退串行扫描，不将节点判为故障。结果无效、超时或节点失联时重试其他节点，最终回退原串行扫描。尚未被协调器提交的预取结果丢失后可重新扫描，不会推进断点。

单个目录有至少 2,048 个条目时，再将该目录按最多 512 个条目一批分配给空闲节点并行读取元数据。结果仍按原名称顺序交给协调器提交；恢复时并行核对断点之前的目录项，已提交文件不会重新进入清单，目录项仍按原规则遍历。恢复前缀期间不会因为已完成文件计数暂时不增长而误触发吞吐回退。批次结果无效、节点失联或池停用时，该目录剩余部分自动回退本地分类，已提交的清单不受影响。

## 在线调节和保护

本轮控制文件为 `runs/presence_recheck_20261109/parallel-scan-tuning.json`；后台每 5 秒读取，无需重启。`enabled` 控制启停，`max_nodes` 的有效范围是 1–16，分别适用于两个来源。每节点每来源同时扫描不超过一个目录或批次。写入时应原子替换整个 JSON，保留其他字段；配置读取故障持续 120 秒时自动关闭远端预取，回到串行扫描。

节点失联、I/O PSI 达到 `io_limit_pct`、匿名内存达到 `memory_limit_pct`、内存压力过高或发现新的 OOM Kill 时，该节点暂停领取；网络或扫描错误使该节点冷却 `cooldown_seconds`。协调器出现相同资源压力时两个扫描池冷却。实际完成文件速度连续两个采样窗口低于 `minimum_completed_files_s` 时，远端扫描池冷却，然后自动重新尝试；图片目录结果可能按大批次返回，使用至少 180 秒的吞吐窗口，避免刚启动时误触发。图片目录默认超时 120 秒，可通过在线配置 `image_timeout_seconds`（10–300 秒）调整。`prepared_datasets` 仍在扫描时，`yuanhai` 单独使用较高的 `coexist_minimum_completed_files_s` 阈值：整体速度持续偏低时先冷却 `yuanhai` 的额外并发，保护已在运行的主任务。正在进行的有界只读请求可完成；这些保护不撤销已提交的上传或进度。

自动调节器根据持续工作量、完成速度和压力在线调整 `max_nodes`，目前保守范围为 2–8：压力升高或试探后的吞吐持续下降时减半；压力较低且工作量持续时才尝试提高。16 个 CPU 节点仍共享工作队列，`max_nodes` 只限制同时执行扫描的节点数。调节器在三个协调候选节点上主备运行，锁保证同一时间只有一个实例修改配置；节点恢复后由 supervisor 拉起。用户直接更改扫描并发或禁用扫描时，调节器停止接管，保留用户设置。状态记录在 `runs/presence_recheck_20261109/monitoring/automatic-scan-tuning.json`。

`status.json` 的 `parallel_scan` 和 `parallel_yuanhai_scan` 记录并发上限、活动节点、排队任务、回退次数及各节点计数。SwanLab 每 120 秒沿用现有实验上报 `scan_fleet/*` 和 `scan_fleet_yuanhai/*`；`resuming_entries` 表示正在核对断点之前的目录项。各节点 `files` 是预扫描计数，可能在恢复后重做，**不是云端已确认的文件数**；`subtrees` 包含单目录条目批次。真实完成进度仍以 `progress/completed_files` 为准。
## Directory-link follow-up scans

`parallel_link_scan_tuning_file` enables the CPU-host link scanner for newly
created follow-up rounds. It does not interrupt an existing serial round.
Idle hosts pull bounded batches from one shared queue; results enter the upload
queue in ledger order. A slow or unavailable host therefore receives less work.
Each host performs source metadata reads only. Uploads retain the original nine
source mappings and the existing dynamic upload queue.

The tuning JSON accepts `enabled`, `max_nodes` (1–16), `batch_links` (1–256,
default 128), and `timeout_seconds` (10–300, default 90). Enablement, concurrency,
and timeout can change online. Each source persists its initial batch size so
crash recovery keeps deterministic upload task IDs. Resource pressure lowers
the effective concurrency automatically, and sustained low pressure lets it
recover within the configured cap. Failed hosts back off; a failed, timed-out,
or oversized batch falls back to the scoped serial scanner.

The local durable queue stores the immutable ledger hash, next committed link
index, exclusion count, audit-file offset, and batch size. A cursor advances only
after its upload manifests are queued and its exclusions are flushed and
synced. Recovery truncates an uncommitted audit tail and resumes at that cursor.
Changed ledgers or missing committed audit bytes stop the source with an error.
Legacy rounds without these cursors continue their original replay behavior.

The root controller owns full-round scan tuning and follows the active global
round. Child full rounds do not start a second tuner against the same settings.
Directory-link children do not recursively launch further global or link
controllers; their parent already owns that repetition.

`parallel_link_scan` and `link_scan_progress` appear in `status.json`; SwanLab
reports `link_scan_fleet/*` host activity, load distribution and failures, plus
`link_scanning/<source>/*` durable link counts and coverage. Reporting retains
the existing minimum 120-second interval. Link coverage is separate from file
coverage because one directory link can contain many files.

