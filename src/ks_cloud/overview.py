"""Overall upload coverage and a self-contained SwanLab HTML dashboard."""
from datetime import datetime, timedelta, timezone
from html import escape


GUIDE = {
    'recovery': [
        ('status_stale','恢复状态后台采集尚无数据或超过 90 秒未刷新时为 1；此时不报告旧的守护在线数。'),
        ('status_age_seconds','恢复状态后台采样开始至今的秒数；辅助采集阻塞不会阻止上传进度更新。'),
        ('leader_index','当前协调器在 hosts 列表中的索引，0 为第一台 CPU 节点；-1 表示未知。'),
        ('generation','协调器任期累计编号，进程重启或接管后增加；原主恢复不抢占当前主。'),
        ('leader_heartbeat_age_seconds','距主节点最近一次成功发布共享心跳的秒数；默认 45 秒失效后拒绝 RPC。'),
        ('guardians_alive','最近 90 秒有守护状态上报的远端节点数量，目标为 16。'),
        ('queue_snapshot_age_seconds','距最近共享队列快照的秒数；恢复可能重做快照之后的增量检查。'),
        ('claim_grace_seconds','从共享快照接管后的任务领取隔离窗口剩余秒数；窗口内继续接受心跳及结果，不发新任务。'),
    ],
    'progress': [
        ('completed_files', '完整成功分片中的已处理文件数，包含实际上传和增量跳过。'),
        ('completed_TiB', '已处理文件对应的数据量（TiB），包含跳过，不是网络上传量。'),
        ('discovered_files', '扫描已发现并提交任务池的文件数；扫描中分母仍会增长。'),
        ('discovered_TiB', '已发现文件对应的数据量（TiB），扫描结束前不代表全部数据。'),
        ('scan_complete', '扫描线程是否全部结束：0 否、1 是；扫描报错也可能结束，需结合异常状态。'),
        ('coverage_of_discovered_pct', '已完成文件数 / 已发现文件数 × 100%，仅表示已发现范围的覆盖率。'),
        ('uploaded_files_confirmed', '本轮实际上传成功且整片确认成功的文件数，不包含跳过。'),
        ('uploaded_TiB_confirmed', '本轮实际上传成功且整片确认成功的数据量（TiB），不包含跳过。'),
        ('skipped_files', '按增量规则无需重传的文件数，不代表重新做了全量内容哈希校验。'),
    ],
    'throughput': [
        ('completed_files_s', '相邻快照的已处理文件数差分 / 秒数，包含上传与跳过。'),
        ('completed_MiB_s_including_skips', '处理量差分速度（MiB/s），包含跳过，不能当作上传带宽。'),
        ('uploaded_MiB_s_confirmed', '整片成功后确认的实际上传量差分速度（MiB/s），短期会有阶梯和尖峰。'),
    ],
    'cluster': [
        ('healthy_nodes', '最近 90 秒有心跳的节点数，目标 16；在线不等于正在上传。'),
        ('active_slots', '最近心跳确认的活动分片数，含等待结果确认；配置总上限 32。'),
        ('leased_chunks', '尚未结束或回收的分片租约数，可能含回复丢失的领取，不等于实际活动数。'),
        ('pending_chunks', '已生成清单、等待领取或退避重试的分片数；不包含尚未扫描到的数据。'),
        ('failed_chunks', '重试耗尽后最终失败的分片数量，不是失败文件数或失败尝试数。'),
        ('scan_errors_or_exclusions', '带扫描异常或排除说明的数据源数量，不是异常文件条数。'),
        ('terminal', '运行是否进入结束状态：0 否、1 是；带异常结束也会为 1。'),
        ('slot_imbalance', '各节点活动槽位数的最大值减最小值；全部空闲也为 0，不衡量字节吞吐均衡。'),
    ],
    'nodes/<节点名>': [
        ('healthy', '最近 90 秒有心跳为 1，否则为 0。'),
        ('active_slots', '本节点心跳确认的活动分片数，正常为 0–2。'),
        ('completed_files', '最终由本节点成功确认的累计处理文件数，包含跳过。'),
        ('completed_GiB', '最终由本节点成功确认的累计处理量（GiB），包含跳过。'),
        ('cpus', '容器 CPU 配额与亲和性共同限制下的可用核数，本批为 4。'),
        ('cpu_used_cores', '两次采样间容器 CPU 时间差分 / 秒数；1 表示平均用满一核，包含容器内其他任务。'),
        ('cpu_throttled_seconds', '容器累计 CPU 限流时间（秒），看增量判断限流强度；不是 OOM。'),
        ('load1', '宿主机一分钟系统负载，含其他容器和 I/O 等待，不能直接和本容器 4 核比较。'),
        ('memory_limit_GiB', '容器内存上限（GiB），本批为 16。'),
        ('memory_current_GiB', '容器当前计费内存（GiB），包含匿名内存、文件缓存和内核等开销。'),
        ('memory_anon_GiB', '容器匿名内存（GiB），主要是进程堆栈，包含容器内其他任务；不等同进程 RSS。'),
        ('memory_file_cache_GiB', '容器文件缓存（GiB），其中相当部分可被回收；高缓存本身不代表 OOM。'),
        ('memory_used_pct', '修复后表示匿名内存 / 容器内存上限 × 100%，不包含文件缓存。'),
        ('memory_total_pct', '容器计费内存 / 容器上限 × 100%，包含文件缓存，接近 100% 不直接证明 OOM。'),
        ('oom_events', '容器生命周期累计 OOM 事件数；不一定每次都杀死进程。'),
        ('oom_kill_events', '容器生命周期累计 OOM 杀进程计数；容器重建后可能归零。'),
        ('network_tx_MiB_s', '节点网络命名空间平均发送速度（MiB/s），包含其他任务和重试流量。'),
        ('network_rx_MiB_s', '节点网络命名空间平均接收速度（MiB/s），包含其他任务流量。'),
    ],
    'overview': [
        ('waiting_for_scan', '1 表示已生成任务均已处理，上传节点正在等待扫描器提供新任务；节点在线不代表上传仍在推进。'),
        ('upload_idle_minutes', '距最近一个成功分片完成的分钟数；初始尚无成功分片时为 0，需结合 waiting_for_scan 和扫描状态。'),
        ('stalled_scanners', '当前扫描操作超过 180 秒没有更新的扫描器数量，不含正常的队列预取等待。'),
        ('coverage_bytes_pct', '全量数据量覆盖率；扫描中使用历史清点参考总量（估算），扫描全部成功结束后用本轮扫描总量。'),
        ('coverage_files_pct', '全量文件数覆盖率；分母规则同上。文件数与字节数百分比可能差别很大。'),
        ('total_is_exact', '1 表示所有数据源扫描结束、总量来自本轮清单（排除项另列）；0 表示分母仍为参考值或未知。'),
        ('total_TiB', '看板所用总数据量（TiB）；是否确切由 total_is_exact 指示。'),
        ('total_files', '看板所用总文件数；是否确切由 total_is_exact 指示。'),
        ('completed_TiB', '整片成功确认的累计检查与补传量（TiB），包含跳过。'),
        ('completed_files', '整片成功确认的累计处理文件数，包含跳过。'),
        ('remaining_TiB', '总数据量减已完成数据量（TiB）；扫描中是参考剩余检查量，不是剩余上传量。'),
        ('actual_uploaded_TiB', '本轮确认实际上传的数据量（TiB），不含跳过。'),
        ('skipped_TiB', '本轮确认增量跳过的数据量（TiB），工具字节数有显示精度限制。'),
        ('scan_finished_sources', '扫描状态为 done 的数据源数量；有排除项的数据源也可为 done。'),
        ('source_count', '配置的数据源总数，本次为 9。'),
        ('healthy_nodes', '最近 90 秒有心跳的节点数，目标 16。'),
        ('active_slots', '最近心跳确认的活动分片数，总上限 32。'),
        ('failed_chunks', '最终失败的分片数。'),
        ('oom_kill_events', '所有节点容器 OOM Kill 累计计数之和；容器重建会影响连续性。'),
        ('snapshot_age_seconds', '看板生成时间距任务快照的秒数；过大说明监控数据可能陈旧。'),
        ('snapshot_stale', '1 表示快照已过期（默认超过 90 秒）；运行中暂停上报旧的进度、吞吐和节点数值，仅保留新鲜度与带警告的看板。'),
        ('all_sources_successful', '仅完整无异常结束时为 1；未结束或存在失败/排除项时为 0。'),
        ('dashboard', '总进度 HTML 看板：参考/确切进度、实际上传量、各目录、节点负载和异常。'),
        ('metric_guide', '所有图表的逐项说明；节点同名指标共享定义，适用于全部 16 个节点。'),
    ],
}


def snapshot_freshness(snapshot, config, now):
    age = max(0, now-snapshot['updated'])
    limit = max(1, config.get('snapshot_stale_after_seconds',
                             max(90, 3*config.get('coordinator_publish_seconds', 30))))
    return {'overview/snapshot_age_seconds': age,
            'overview/snapshot_stale': int(age > limit)}


def guide_markdown():
    text = ['# SwanLab 上传图表逐项说明', '',
            '每约 120 秒一个点；Step 为 Unix 秒时间戳。累计完成量在整片成功后入账，因此会呈阶梯变化。', '',
            '已处理包含增量跳过；本轮实际上传不含跳过。扫描未完成时，总览使用历史清点参考量，明确标为估算。', '',
            '2026-11-03 凌晨修复前，CPU/内存来自宿主机、活动数包含遗留租约；修复后按容器配额和心跳确认统计，前后不能直接比较。', '']
    for group, entries in GUIDE.items():
        text += [f'## {group}', '', '| 图表 | 含义 |', '|---|---|']
        text += [f'| `{group}/{key}` | {description} |' for key, description in entries]
        text += ['']
    return '\n'.join(text)


def build_overview(snapshot, config, inventory=None, now=None):
    now = snapshot['updated'] if now is None else now
    inventory = inventory or {}
    scans = {s['source']: s for s in snapshot['scans']}
    rows = []
    for task in config['tasks']:
        name = task['id']
        groups = [g for g in snapshot['groups'] if g['source']==name]
        scan = scans.get(name, {})
        discovered = {k:sum(g[k] for g in groups) for k in ['files','bytes']}
        completed = {k:sum(g[k] for g in groups if g['state']=='done') for k in ['files','bytes']}
        exact = scan.get('state')=='done'
        ref = inventory.get('sources',{}).get(name,{})
        valid_ref = ref.get('source')==task['source'] and ref.get('complete') is True
        totals = discovered if exact else ({k:max(discovered[k],ref[k]) for k in ['files','bytes']} if valid_ref else None)
        rows.append(dict(name=name, scan=scan.get('state','pending'), error=scan.get('error',''),
                         discovered=discovered, completed=completed, total=totals,
                         exact=exact, failed=sum(g['chunks'] for g in groups if g['state']=='failed')))
    available = all(r['total'] is not None for r in rows) and bool(rows)
    exact = all(r['exact'] for r in rows) and bool(rows)
    total = {k:sum(r['total'][k] for r in rows) for k in ['files','bytes']} if available else None
    counters = snapshot['counters']
    metrics = {
        'overview/waiting_for_scan': int(snapshot.get('waiting_for_scan', False)),
        'overview/upload_idle_minutes': (snapshot.get('upload_idle_seconds') or 0)/60,
        'overview/stalled_scanners': len(snapshot.get('stalled_scanners', [])),
        'overview/total_is_exact': int(exact),
        'overview/completed_TiB': snapshot['completed_bytes']/1024**4,
        'overview/completed_files': snapshot['completed_files'],
        'overview/actual_uploaded_TiB': counters.get('succeeded_bytes',0)/1024**4,
        'overview/skipped_TiB': counters.get('skipped_bytes',0)/1024**4,
        'overview/scan_finished_sources': sum(r['scan']=='done' for r in rows),
        'overview/source_count': len(rows),
        'overview/healthy_nodes': sum(n['healthy'] for n in snapshot['nodes'].values()),
        'overview/active_slots': len(snapshot['active']),
        'overview/failed_chunks': sum(r['failed'] for r in rows),
        'overview/oom_kill_events': sum(n.get('oom_kill_events',0) for n in snapshot['nodes'].values()),
        **snapshot_freshness(snapshot, config, now),
        'overview/all_sources_successful': int(snapshot['state']=='finished' and exact and not any(r['error'] or r['failed'] for r in rows)),
    }
    if available:
        for kind, key in [('bytes','coverage_bytes_pct'),('files','coverage_files_pct')]:
            completed = snapshot['completed_bytes' if kind=='bytes' else 'completed_files']
            metrics['overview/'+key] = 100*completed/total[kind] if total[kind] else (100 if exact else 0)
        metrics.update({'overview/total_TiB': total['bytes']/1024**4,
                        'overview/total_files': total['files'],
                        'overview/remaining_TiB': max(0,total['bytes']-snapshot['completed_bytes'])/1024**4})
    return dict(metrics=metrics, rows=rows, total=total, exact=exact, snapshot=snapshot,
                state=snapshot['state'], updated=snapshot['updated'])


STYLE = '''body{font:14px system-ui,sans-serif;background:#f5f7fb;color:#172338;margin:0;padding:24px}
*{box-sizing:border-box}h1{font-size:24px}h2{font-size:18px;margin-top:28px}.muted{color:#586780;line-height:1.8}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px}.card{background:white;border:1px solid #dfe6f1;border-radius:12px;padding:17px}
.value{font-size:27px;font-weight:700;margin:8px 0}.label{font-size:13px;color:#586780}
.notice{background:#fff2cc;border-left:4px solid #d99905;padding:13px;line-height:1.7}.bar{height:12px;background:#dfe6f1;border-radius:8px;overflow:hidden;margin:10px 0}.bar i{display:block;height:100%;background:#3369dc}
table{border-collapse:collapse;width:100%;background:white;margin:12px 0}td,th{text-align:left;border-bottom:1px solid #e1e6ed;padding:10px;vertical-align:top}th{background:#edf2fa}code{font-size:12px;overflow-wrap:anywhere}.scroll{overflow-x:auto}
details{background:white;padding:12px;margin-top:12px;border-radius:8px}summary{cursor:pointer;font-weight:600}a{color:#2358bb}'''


def document(title, body):
    return f'<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{escape(title)}</title><style>{STYLE}</style><body>{body}</body></html>'


def render_guide():
    body = '<h1>上传图表逐项说明</h1><p class="muted">每约 120 秒更新。Step 为 Unix 秒时间戳；GiB=1024³ 字节，TiB=1024⁴ 字节。已处理包含跳过，实际上传不含跳过。</p>'
    body += '<div class="notice">历史口径变化：2026-11-03 凌晨修复前，CPU/内存来自宿主机、活动数包含遗留租约；修复后改为容器配额、匿名内存比例及心跳确认。修复前后的曲线不能直接比较。</div>'
    for group, entries in GUIDE.items():
        body += f'<h2>{escape(group)}</h2><table><tr><th>图表完整名称</th><th>含义与读法</th></tr>'
        body += ''.join(f'<tr><td><code>{escape(group+"/"+key)}</code></td><td>{escape(description)}</td></tr>' for key,description in entries)
        body += '</table>'
    body += '<p class="muted">nodes/&lt;节点名&gt; 适用于 cpu-master-0 和 cpu-worker-0 至 cpu-worker-14。同名指标含义完全相同。CPU、内存与网络指标包含容器内其他任务。</p>'
    return document('SwanLab 上传图表逐项说明',body)


def render_overview(report):
    s, m = report['snapshot'], report['metrics']
    ts = datetime.fromtimestamp(report['updated'],timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S 北京时间')
    basis = '本轮扫描总量' if report['exact'] else '历史清点参考总量（估算）'
    body = f'<h1>16 节点上传 · 总进度看板</h1><p class="muted">快照：{ts} · 状态：{escape(report["state"])} · 约每 120 秒上传一份新快照，请查看最新 step。</p>'
    if s.get('recovery_status_stale'):
        body += '<p>恢复状态采集尚未完成或已过期；守护在线数暂不可用，上传进度仍独立更新。</p>'
    if s.get('recovery'):
        recovery=s['recovery'];leader=recovery['leader']
        body += '<p>远端协调器：'+escape(leader.get('host','未知'))+f' · 任期 {leader.get("generation",0)} · 守护在线 {sum(g["alive"] for g in recovery["guardians"].values())}/{len(recovery["guardians"])}</p>'
    if not report['exact']:
        body += '<div class="notice">全量扫描尚未结束。以下总覆盖率使用历史清点参考量，不是精确完成率；已发现范围接近 100% 不代表全部数据已处理。各目录扫描完成后，其分母自动采用本轮实际扫描总量。</div>'
    if m['overview/snapshot_stale']:
        body += f'<div class="notice">当前任务快照已过期（{m["overview/snapshot_age_seconds"]:.0f} 秒未刷新）。以下为最后已知状态，不能据此判断当前上传速度或节点是否在线。</div>'
    if m['overview/waiting_for_scan']:
        body += f'<div class="notice">上传队列为空，正在等待扫描器提供新任务；距最后完成分片已 {m["overview/upload_idle_minutes"]:.1f} 分钟。节点在线不代表上传仍在推进。</div>'
    if m['overview/stalled_scanners']:
        body += '<div class="notice">扫描操作超过阈值未更新：'+escape(', '.join(s['stalled_scanners']))+'。请检查下方扫描器当前位置及共享盘。</div>'
    def card(label,value,description=''):
        return f'<div class="card"><div class="label">{escape(label)}</div><div class="value">{escape(str(value))}</div><div class="muted">{escape(description)}</div></div>'
    pct = m.get('overview/coverage_bytes_pct')
    body += '<div class="cards">'+card('全量数据量覆盖率',f'{pct:.2f}%' if pct is not None else '总量待确定',basis)
    body += card('已处理 / 总数据量',f'{m["overview/completed_TiB"]:.2f} / {m["overview/total_TiB"]:.2f} TiB' if report['total'] else f'{m["overview/completed_TiB"]:.2f} TiB','包含增量跳过，整片成功后入账')
    body += card('本轮实际上传',f'{m["overview/actual_uploaded_TiB"]:.2f} TiB','不包含已存在对象的跳过')
    body += card('增量跳过',f'{m["overview/skipped_TiB"]:.2f} TiB','按更新规则跳过，非重新做全量哈希')
    body += card('在线节点 / 活动槽位',f'{m["overview/healthy_nodes"]} / {m["overview/active_slots"]}','配置目标：16 节点、最多 32 槽位')
    body += card('扫描完成目录',f'{m["overview/scan_finished_sources"]} / {m["overview/source_count"]}','done 可能含排除项，见下表')
    body += card('最终失败分片 / OOM Kill',f'{m["overview/failed_chunks"]} / {m["overview/oom_kill_events"]}','排除项单独列出；容器重建可能重置 OOM 计数')+'</div>'
    if pct is not None:
        body += f'<div class="bar"><i style="width:{min(100,max(0,pct)):.2f}%"></i></div>'
    files_pct = m.get('overview/coverage_files_pct')
    body += f'<p class="muted">文件维度：已处理 {s["completed_files"]:,} 个'
    if report['total']:
        body += f' / {report["total"]["files"]:,} 个（{files_pct:.2f}%，{basis}）'
    body += f'。已发现 {s["discovered_files"]:,} 个、{s["discovered_bytes"]/1024**4:.2f} TiB。扫描未完成前不提供全量 ETA。</p>'
    body += '<h2>9 个目录的覆盖情况</h2><div class="scroll"><table><tr><th>目录</th><th>扫描</th><th>已处理 / 总量 TiB</th><th>数据量覆盖率</th><th>已处理 / 总文件数</th><th>分母依据 / 异常</th></tr>'
    labels={'scanning':'扫描中','done':'扫描结束','pending':'待扫描','error':'扫描出错'}
    for r in report['rows']:
        total = r['total']
        percent = 100*r['completed']['bytes']/total['bytes'] if total and total['bytes'] else (100 if r['exact'] else None)
        byte_text = f'{r["completed"]["bytes"]/1024**4:.3f} / {total["bytes"]/1024**4:.3f}' if total else f'{r["completed"]["bytes"]/1024**4:.3f} / 待确定'
        file_text = f'{r["completed"]["files"]:,} / {total["files"]:,}' if total else f'{r["completed"]["files"]:,} / 待确定'
        note=('本轮扫描' if r['exact'] else '历史参考（估算）' if total else '缺少参考总量')
        if r['error']:note += '；'+r['error']
        if r['failed']:note += f'；最终失败 {r["failed"]} 片'
        body += '<tr>'+''.join(f'<td>{escape(str(value))}</td>' for value in [r['name'],labels.get(r['scan'],r['scan']),byte_text,f'{percent:.2f}%' if percent is not None else '待确定',file_text,note])+'</tr>'
    body += '</table></div><h2>节点资源与负载</h2><table><tr><th>节点</th><th>在线 / 活动槽位</th><th>匿名内存 / 容器上限 GiB</th><th>文件缓存 GiB</th><th>OOM Kill</th></tr>'
    for host,n in sorted(s['nodes'].items()):
        values=[host,f'{"在线" if n["healthy"] else "心跳过期"} / {n.get("active_slots",0)}',
                f'{n.get("memory_anon_GiB",0):.2f} / {n.get("memory_limit_GiB",0):.0f}',f'{n.get("memory_file_cache_GiB",0):.2f}',n.get('oom_kill_events',0)]
        body += '<tr>'+''.join(f'<td>{escape(str(value))}</td>' for value in values)+'</tr>'
    body += '</table><details><summary>如何读总进度</summary><p>覆盖率衡量已检查和补传的范围，包含跳过；实际上传量只统计本轮补传。历史总量不是当前文件系统快照，数据变化会影响估算。扫描结束不等于无异常完成，需查看最终失败和排除项。</p><p>每张原始图表的完整定义见同一 overview 分组下的 metric_guide。</p></details>'
    return document('16 节点上传总进度',body)
