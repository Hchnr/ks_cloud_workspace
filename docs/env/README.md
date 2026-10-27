# 新环境准备

## 1. 安装基础工具

控制机和全部 worker 需要 Linux、Python 3.10+、OpenSSH 客户端；worker 需要 SSH 服务。控制机另需 Git。Python 代码仅使用标准库，不需要 `pip install`。

Debian/Ubuntu 示例（管理员执行）：

```bash
sudo apt-get update
sudo apt-get install -y python3 openssh-client git
# 仅在需要作为 worker 提供 SSH 服务的节点安装
sudo apt-get install -y openssh-server
```

安装后检查 `python3 --version`、`ssh -V`、`git --version`。

## 2. 安装并配置 ks3util

从金山云官方文档或组织批准的软件源取得对应架构的 `ks3util`，在所有节点安装到相同的绝对路径。本任务验证过的版本为 **2.17.0**。不要使用来历不明的二进制。

```bash
install -m 755 /path/to/downloaded/ks3util "$HOME/.local/bin/ks3util"
/path/to/ks3util --help
/path/to/ks3util cp --help
```

若 `$HOME/.local/bin` 尚不存在，先 `mkdir -p "$HOME/.local/bin"`。

按所安装版本的交互配置流程，创建私有 `.ks3util.conf`，设置 AccessKey/SecretKey、endpoint、region 及必要的代理。配置文件权限建议 `chmod 600`；所有节点必须能读取配置中的绝对路径。**不要把密钥写进 JSON 示例、脚本、Git 或终端日志。**

使用具有目标桶写入、查询及分片上传所需权限的账号；增量检查需要查询已有对象。内网 endpoint 只有可达相应内网的环境才能使用。旧环境通过 HTTP 代理访问 KS3，新环境应独立确认网络路径，不要照搬旧 IP。

先按 `ks3util` 帮助中的 `ls` / `stat` 语法对目标桶进行只读访问测试；再用一个小样本验证预期的对象路径和更新跳过行为，最后运行大规模计划。旧机器上的配置和 PDF 不会自动迁移。

## 3. 配置 SSH 和共享数据

在控制机配置 SSH 密钥和 `~/.ssh/config` 中的节点别名。为每台节点核实主机密钥后，确保以下命令不需要交互：

```bash
ssh -o BatchMode=yes master-0 'python3 --version'
ssh -o BatchMode=yes worker-0 'python3 --version'
```

不要把私钥提交到仓库，也不要为方便连接而禁用主机密钥校验。节点别名要唯一，并实际指向不同节点，避免两台别名落在同一节点的同一个运行目录。

所有节点必须以相同绝对路径访问源目录、对象清单、ks3util 和私有配置；控制机生成的清单路径通常位于共享文件系统，因此 `--output` 也应位于所有节点可读的路径。代码由控制机通过 SSH 部署，不要求远端安装这个仓库。

## 4. 选择运行目录和检查容量

`remote_run_dir` 是每台节点的本地目录，例如 `/var/tmp/ks-cloud/recheck-20261027`。每次全新复查使用新目录；断点恢复使用原目录和原计划。记录日志、结果和大文件分片断点需要可写空间；用 `df -h`、`df -i` 并结合账号/目录配额检查。曾发生过 `df` 有剩余但写入返回 `ENOSPC`，不能只看全盘剩余量。

`/tmp` 在重启或容器重建后可能丢失；长期任务优先使用节点持久盘。不要把多台节点的 `remote_run_dir` 指向同一个共享目录，任务锁和执行状态设计为每节点独立。

最后复制 `examples/config.json` 为 `config.local.json`，配置全部24个节点（master 加23个 worker 时共24个），再按仓库 README 生成、审阅和启动计划。

## 私有参考资料

本目录现存的 `cuihu_ks3.pdf`、`cuihu_user.pdf` 是环境内部参考资料，默认被 Git 忽略；可能含内部配置，需通过组织授权渠道另行获取，不应加入公开仓库。
