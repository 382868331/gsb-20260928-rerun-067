# 实验结果复现系统

这是一个无第三方依赖的 Python 独立项目。它把一次实验的四类信息绑定成不可混淆的记录：

- 输入：记录每个输入的字节长度与 SHA-256；文件名只作为来源展示，不参与缓存判断。
- 参数：使用稳定排序的规范化 JSON 参与身份计算。
- 运行环境说明：记录模式版本、后端标识、Python 版本和运行平台；默认不读取环境变量或用户配置清单。
- 产物：记录发布时逐个计算 SHA-256，并由记录清单统一引用。

## 语义

- 相同输入内容、参数和环境说明直接复用已有完整记录。
- 输入内容、参数或环境说明变化会得到新的请求身份并重新执行。
- 后端函数抛出异常时只清理暂存区，不会发布可缓存结果。
- 记录先写入暂存目录，产物和清单完整落盘后再原子发布；写入中断不会破坏上一份完整记录。
- 相同请求通过按请求粒度的文件锁（locks/<hash>.lock）串行化，保证只有一份有效产物，支持多线程和多进程；不同请求可并行执行。
- 每次执行尝试都会追加到 attempts/<hash>.jsonl，包含执行者标识、进程号、开始/发布/失败/复用事件。
- 执行者中途退出后，接管方在 attempts 日志中记录 takeover_basis（孤儿尝试、残留暂存目录、无记录的产物、校验失败的记录），清理残留后重新执行。
- 暂存目录按 staging/<hash>/<attempt_id> 隔离，正在写入的目录永远不会被当作缓存命中。
- 重新打开存储时逐个校验产物的 SHA-256，损坏产物会被识别并触发重新执行，不会无条件复用。
- 计算后端位于独立接口之后；后续替换后端时，记录字段和身份语义保持不变。

## 可移交冻结包

选定候选结果可以冻结成一份可移交的交付目录（experiment-delivery/v1）：

- 冻结前用 freeze-plan 预览：每个候选被分类为 confirmed（记录完整且校验通过）、needs_rerun（无完整记录）、unproven（目录里有同名文件但无法证明来源）或 blocked（共享依赖损坏、输入在运行后被替换、仍有阶段未结束）。
- 只有 confirmed 的候选会被装入交付；被阻断的候选及其原因写进 delivery.json，其余候选正常交付，互不影响。
- 候选可以声明 depends_on 共享中间记录，冻结时重新核验；--derive/--set 支持只采纳某候选的一部分参数决定，派生出的新身份必须先有完整记录才能交付。
- 交付目录包含 records/（记录清单）、artifacts/（产物）、inputs/（输入快照，接收方无需猜原文件路径）、delivery.json（选择理由与版本差异）和 manifest.json（逐文件 SHA-256）；目录一旦写出即固定，输出目录非空时拒绝覆盖。
- 每次导出在源存储的 deliveries/ 中登记递增版本，新版本的 changes 字段说明相对上一版的新增、移除与替换及其依据。
- 接收方 freeze-verify 逐文件校验完整性，freeze-import 只安装全部校验通过的记录；记录身份由请求内容重算，重复导入同一份交付只会复用已有记录，不产生第二套来源；材料不全的交付不会被标成完整实验。

    python -m exp_records.cli freeze-plan --store ./store --select cand-a=<hash> --reason cand-a=采用基准参数 --input cand-a:numbers=./numbers.txt
    python -m exp_records.cli freeze-export --store ./store --out ./bundle --select cand-a=<hash> --input cand-a:numbers=./numbers.txt
    python -m exp_records.cli freeze-verify --bundle ./bundle
    python -m exp_records.cli freeze-import --store ./receiver-store --bundle ./bundle

## 本地运行

    python -m exp_records.cli run --store ./example-store --input numbers=./numbers.txt --param multiplier=2

多次执行同一命令会输出 reused=true。把文件换成不同路径但保持内容相同仍会复用；改变任意字节则会重新执行。

## 测试

    python -m unittest discover -s tests -v
