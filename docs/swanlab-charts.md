# SwanLab 上传图表逐项说明

每约 120 秒一个点。当前视图采用 Relative Time 横轴，自动以小时（Hour）或天（Day）显示指标记录时长；不同指标启用时间可能不同，不能直接用相同刻度对齐事件。底层 Step 仍保存 Unix 秒时间戳，累计完成量在整片成功后入账，因此会呈阶梯变化。北京时间可在总进度 HTML 看板查看。

2026-11-04 原位调整现有图表和默认视图，保留历史数据。当前网页折线图尚未实现绝对日期轴 `time`；虽然 API 能按时间戳取数，直接设置会造成前端异常。因此使用平台已实现的 `relative_time` 系统轴，其描述必须为 `type=SYSTEM, class=SCALAR`，不能使用旧错误的 `type=FLOAT` 图表描述。

已处理包含增量跳过；本轮实际上传不含跳过。扫描未完成时，总览使用历史清点参考量，明确标为估算。

2026-11-03 凌晨修复前，CPU/内存来自宿主机、活动数包含遗留租约；修复后按容器配额和心跳确认统计，前后不能直接比较。

## scanning

| 图表 | 含义 |
|---|---|
| `scanning/control_revision` | 本次协调器启动后有效热配置变更次数；重启归零，不是实验总步数。 |
| `scanning/enabled` | 并行元数据预取开关；0 为串行扫描，上传 worker 继续工作。 |
| `scanning/config_stale` | 热配置无效、不可读或超过有效期时为 1；自动退回串行。 |
| `scanning/config_age_seconds` | 距最近一次完成热配置读取的秒数；独立读取线程不阻塞上传调度。 |

## scanning/<数据源>

| 图表 | 含义 |
|---|---|
| `scanning/<数据源>/workers` | 该源当前允许的元数据预取窗口，1 为串行；全局最多 8 个元数据线程。 |
| `scanning/<数据源>/files_s` | 最近控制周期内扫描识别的文件数/秒；不代表云端已完成上传。 |
| `scanning/<数据源>/upload_files_s` | 最近控制周期内该源完整成功分片的文件数/秒，含增量跳过。 |
| `scanning/<数据源>/metadata_ms` | 元数据操作平均耗时（毫秒）；目录与排除项也计入。 |
| `scanning/<数据源>/operations` | 本次协调器启动后累计检查的目录项数；重启归零。 |
| `scanning/<数据源>/errors` | 本次检查中需排除的目录项累计数；具体原因见排除审计文件。 |
| `scanning/<数据源>/fallbacks` | 本次启动后从并行降回串行的保护次数；队列满正常减速不计入。 |
| `scanning/<数据源>/cooldown_seconds` | 保护回退后剩余冷却秒数；冷却期间不自动提高并发。 |

## recovery

| 图表 | 含义 |
|---|---|
| `recovery/leader_index` | 当前协调器在 hosts 列表中的索引，0 为第一台 CPU 节点；-1 表示未知。 |
| `recovery/generation` | 协调器任期累计编号，进程重启或接管后增加；原主恢复不抢占当前主。 |
| `recovery/leader_heartbeat_age_seconds` | 距主节点最近一次成功发布共享心跳的秒数；默认 45 秒失效后拒绝 RPC。 |
| `recovery/guardians_alive` | 最近 90 秒有守护状态上报的远端节点数量，目标为 16。 |
| `recovery/queue_snapshot_age_seconds` | 距最近共享队列快照的秒数；恢复可能重做快照之后的增量检查。 |
| `recovery/claim_grace_seconds` | 从共享快照接管后的任务领取隔离窗口剩余秒数；窗口内继续接受心跳及结果，不发新任务。 |

## progress

| 图表 | 含义 |
|---|---|
| `progress/completed_files` | 完整成功分片中的已处理文件数，包含实际上传和增量跳过。 |
| `progress/completed_TiB` | 已处理文件对应的数据量（TiB），包含跳过，不是网络上传量。 |
| `progress/discovered_files` | 扫描已发现并提交任务池的文件数；扫描中分母仍会增长。 |
| `progress/discovered_TiB` | 已发现文件对应的数据量（TiB），扫描结束前不代表全部数据。 |
| `progress/scan_complete` | 扫描线程是否全部结束：0 否、1 是；扫描报错也可能结束，需结合异常状态。 |
| `progress/coverage_of_discovered_pct` | 已完成文件数 / 已发现文件数 × 100%，仅表示已发现范围的覆盖率。 |
| `progress/uploaded_files_confirmed` | 本轮实际上传成功且整片确认成功的文件数，不包含跳过。 |
| `progress/uploaded_TiB_confirmed` | 本轮实际上传成功且整片确认成功的数据量（TiB），不包含跳过。 |
| `progress/skipped_files` | 按增量规则无需重传的文件数，不代表重新做了全量内容哈希校验。 |

## throughput

| 图表 | 含义 |
|---|---|
| `throughput/completed_files_s` | 相邻快照的已处理文件数差分 / 秒数，包含上传与跳过。 |
| `throughput/completed_MiB_s_including_skips` | 处理量差分速度（MiB/s），包含跳过，不能当作上传带宽。 |
| `throughput/uploaded_MiB_s_confirmed` | 整片成功后确认的实际上传量差分速度（MiB/s），短期会有阶梯和尖峰。 |

## cluster

| 图表 | 含义 |
|---|---|
| `cluster/healthy_nodes` | 最近 90 秒有心跳的节点数，目标 16；在线不等于正在上传。 |
| `cluster/active_slots` | 最近心跳确认的活动分片数，含等待结果确认；配置总上限 32。 |
| `cluster/leased_chunks` | 尚未结束或回收的分片租约数，可能含回复丢失的领取，不等于实际活动数。 |
| `cluster/pending_chunks` | 已生成清单、等待领取或退避重试的分片数；不包含尚未扫描到的数据。 |
| `cluster/failed_chunks` | 重试耗尽后最终失败的分片数量，不是失败文件数或失败尝试数。 |
| `cluster/scan_errors_or_exclusions` | 带扫描异常或排除说明的数据源数量，不是异常文件条数。 |
| `cluster/terminal` | 运行是否进入结束状态：0 否、1 是；带异常结束也会为 1。 |
| `cluster/slot_imbalance` | 各节点活动槽位数的最大值减最小值；全部空闲也为 0，不衡量字节吞吐均衡。 |

## nodes/<节点名>

| 图表 | 含义 |
|---|---|
| `nodes/<节点名>/healthy` | 最近 90 秒有心跳为 1，否则为 0。 |
| `nodes/<节点名>/active_slots` | 本节点心跳确认的活动分片数，正常为 0–2。 |
| `nodes/<节点名>/completed_files` | 最终由本节点成功确认的累计处理文件数，包含跳过。 |
| `nodes/<节点名>/completed_GiB` | 最终由本节点成功确认的累计处理量（GiB），包含跳过。 |
| `nodes/<节点名>/cpus` | 容器 CPU 配额与亲和性共同限制下的可用核数，本批为 4。 |
| `nodes/<节点名>/cpu_used_cores` | 两次采样间容器 CPU 时间差分 / 秒数；1 表示平均用满一核，包含容器内其他任务。 |
| `nodes/<节点名>/cpu_throttled_seconds` | 容器累计 CPU 限流时间（秒），看增量判断限流强度；不是 OOM。 |
| `nodes/<节点名>/load1` | 宿主机一分钟系统负载，含其他容器和 I/O 等待，不能直接和本容器 4 核比较。 |
| `nodes/<节点名>/memory_limit_GiB` | 容器内存上限（GiB），本批为 16。 |
| `nodes/<节点名>/memory_current_GiB` | 容器当前计费内存（GiB），包含匿名内存、文件缓存和内核等开销。 |
| `nodes/<节点名>/memory_anon_GiB` | 容器匿名内存（GiB），主要是进程堆栈，包含容器内其他任务；不等同进程 RSS。 |
| `nodes/<节点名>/memory_file_cache_GiB` | 容器文件缓存（GiB），其中相当部分可被回收；高缓存本身不代表 OOM。 |
| `nodes/<节点名>/memory_used_pct` | 修复后表示匿名内存 / 容器内存上限 × 100%，不包含文件缓存。 |
| `nodes/<节点名>/memory_total_pct` | 容器计费内存 / 容器上限 × 100%，包含文件缓存，接近 100% 不直接证明 OOM。 |
| `nodes/<节点名>/oom_events` | 容器生命周期累计 OOM 事件数；不一定每次都杀死进程。 |
| `nodes/<节点名>/oom_kill_events` | 容器生命周期累计 OOM 杀进程计数；容器重建后可能归零。 |
| `nodes/<节点名>/network_tx_MiB_s` | 节点网络命名空间平均发送速度（MiB/s），包含其他任务和重试流量。 |
| `nodes/<节点名>/network_rx_MiB_s` | 节点网络命名空间平均接收速度（MiB/s），包含其他任务流量。 |

## overview

| 图表 | 含义 |
|---|---|
| `overview/waiting_for_scan` | 1 表示已生成任务均已处理，上传节点正在等待扫描器提供新任务；节点在线不代表上传仍在推进。 |
| `overview/upload_idle_minutes` | 距最近一个成功分片完成的分钟数；初始尚无成功分片时为 0，需结合 waiting_for_scan 和扫描状态。 |
| `overview/stalled_scanners` | 当前扫描操作超过 180 秒没有更新的扫描器数量，不含正常的队列预取等待。 |
| `overview/coverage_bytes_pct` | 全量数据量覆盖率；扫描中使用历史清点参考总量（估算），扫描全部成功结束后用本轮扫描总量。 |
| `overview/coverage_files_pct` | 全量文件数覆盖率；分母规则同上。文件数与字节数百分比可能差别很大。 |
| `overview/total_is_exact` | 1 表示所有数据源扫描结束、总量来自本轮清单（排除项另列）；0 表示分母仍为参考值或未知。 |
| `overview/total_TiB` | 看板所用总数据量（TiB）；是否确切由 total_is_exact 指示。 |
| `overview/total_files` | 看板所用总文件数；是否确切由 total_is_exact 指示。 |
| `overview/completed_TiB` | 整片成功确认的累计检查与补传量（TiB），包含跳过。 |
| `overview/completed_files` | 整片成功确认的累计处理文件数，包含跳过。 |
| `overview/remaining_TiB` | 总数据量减已完成数据量（TiB）；扫描中是参考剩余检查量，不是剩余上传量。 |
| `overview/actual_uploaded_TiB` | 本轮确认实际上传的数据量（TiB），不含跳过。 |
| `overview/skipped_TiB` | 本轮确认增量跳过的数据量（TiB），工具字节数有显示精度限制。 |
| `overview/scan_finished_sources` | 扫描状态为 done 的数据源数量；有排除项的数据源也可为 done。 |
| `overview/source_count` | 配置的数据源总数，本次为 9。 |
| `overview/healthy_nodes` | 最近 90 秒有心跳的节点数，目标 16。 |
| `overview/active_slots` | 最近心跳确认的活动分片数，总上限 32。 |
| `overview/failed_chunks` | 最终失败的分片数。 |
| `overview/oom_kill_events` | 所有节点容器 OOM Kill 累计计数之和；容器重建会影响连续性。 |
| `overview/snapshot_age_seconds` | 看板生成时间距任务快照的秒数；过大说明监控数据可能陈旧。 |
| `overview/all_sources_successful` | 仅完整无异常结束时为 1；未结束或存在失败/排除项时为 0。 |
| `overview/dashboard` | 总进度 HTML 看板：参考/确切进度、实际上传量、各目录、节点负载和异常。 |
| `overview/metric_guide` | 所有图表的逐项说明；节点同名指标共享定义，适用于全部 16 个节点。 |
