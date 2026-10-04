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

## 本地运行

    python -m exp_records.cli run --store ./example-store --input numbers=./numbers.txt --param multiplier=2

 多次执行同一命令会输出 reused=true。把文件换成不同路径但保持内容相同仍会复用；改变任意字节则会重新执行。
 
 ## 候选结果的冻结与移交
 
 冻结把选定候选（request hash）连同其记录与产物打包成一份可移交材料，接收者无需原工作目录即可逐件核对。
 
- 冻结前先预览：`freeze-plan` 把每个候选分类为 confirmed（记录与产物校验通过）、needs_redo（记录缺失、产物损坏、输入在选择后被替换或仍有阶段未结束）、unproven（只有产物没有可信记录）。候选的交付状态进一步明确为 complete、missing、corrupted，并逐项列出依赖文件；暂缺或损坏只进入包清单的审计部分，不会冒充 records 中可复用的成功记录。
- `freeze` 允许发布部分包：健康候选照常携带可信记录和依赖，其他候选保持 missing/corrupted 状态。多个候选内容相同的共享产物仍按 SHA-256 去重；一个候选的共享依赖损坏不会阻止其他健康候选导入。
 - 选择可以只采纳候选的一部分参数决定：`--selections` JSON 文件中每个条目支持 adopted_parameters（缺省为全部采纳）、reason（采用原因）和 expected_inputs（选择时看到的输入摘要，用于发现输入被替换）。内容相同但来源不同的材料各自记录采用原因。
 - 冻结时重新核对依赖；多个候选共享的内容相同产物在包内按 SHA-256 只存一份。包写入暂存目录后原子发布，package_id 由内容决定，已发布的包不可变，重复冻结同一内容会报错。
 - 后续重新计算或补齐缺失内容时，用 `--supersedes` 指向上一版包生成新版本；新清单记录 version、supersedes 和 changes（新增、移除、替换的候选及依据），原版本保持不变。
- 接收者用 `import-package` 逐候选、逐件验证：每个产物核对 SHA-256，每条记录重算请求哈希并核对产物引用，依赖齐备后才装入本地存储并可被 `run` 复用。部分包只安装健康候选，missing/corrupted 候选保留原因，不影响其他候选。
- 后续补充包可以包含重叠候选：相同成功记录标记 already_present，不产生第二套来源；若补充包携带的候选版本与接收方已经使用的版本不一致，导入报告标记 conflict，列出双方产物路径、大小和 SHA-256，且绝不覆盖已用版本。每次导入都会追加到接收方存储的 `delivery-imports.jsonl`，旧包和旧导入记录保持不变。只要本包仍有 missing/corrupted/conflict 项，`verified_complete=false`；这只表示本次交付尚未完整，不撤销已经成功安装的健康候选。
 
     python -m exp_records.cli freeze-plan --store ./example-store --select <hash>
     python -m exp_records.cli freeze --store ./example-store --output ./packages --select <hash>
     python -m exp_records.cli import-package --store ./receiver-store --package ./packages/<package_id>
 
 ## 测试

    python -m unittest discover -s tests -v
