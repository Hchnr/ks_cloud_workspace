# 金山云备份范围

本任务只上传以下九个源路径，保持其在 `ks3://baai-eai/backup_20261023/` 下的原完整路径；完整映射见 [docs/dynamic-upload.md](docs/dynamic-upload.md)：

- `/share-mixed/project/eai_pwm/pipeline_store/materialized/koala-36m_v2`
- `/share-mixed/project/eai_pwm/pipeline_store/latents`
- `/share-mixed/project/eai_pwm/prepared_datasets`
- `/share-mixed/project/eai_pwm/datasets/train`
- `/share-mixed/project/eai_pwm/pipeline_store/materialized/action100m_v2`
- `/share-mixed/projset/eai_pwm/datasets/Sekai`
- `/share-mixed/project/yhzhang/data/Eidon-Tracker-POV`
- `/share-mixed/project/eai_pwm/home/yuanhai`
- `/share-mixed/project/yhzhang/data/Eidon-Tracker-POV/hand_pose`

用户于 2026-11-10 明确：文件或目录软链接的实际目标若在上述九个目录之外，应跳过，不上传该目标的内容；目标在九个目录之内的链接仍按原逻辑路径处理。未解析或失效的链接应单独记录，不能作为已经备份的证据。已有云端对象不要因本条规则自动删除。
