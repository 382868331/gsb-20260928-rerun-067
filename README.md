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
 
 - 冻结前先预览：`freeze-plan` 把每个候选分类为 confirmed（记录与产物校验通过）、needs_redo（记录缺失、产物损坏、输入在选择后被替换或仍有阶段未结束）、unproven（只有产物没有可信记录）。任何非 confirmed 的候选都会使冻结中止并明确列出，不会把目录里现成的同名文件装进包，也不会混入其他候选未确认的结果。
 - 选择可以只采纳候选的一部分参数决定：`--selections` JSON 文件中每个条目支持 adopted_parameters（缺省为全部采纳）、reason（采用原因）和 expected_inputs（选择时看到的输入摘要，用于发现输入被替换）。内容相同但来源不同的材料各自记录采用原因。
 - 冻结时重新核对依赖；多个候选共享的内容相同产物在包内按 SHA-256 只存一份。包写入暂存目录后原子发布，package_id 由内容决定，已发布的包不可变，重复冻结同一内容会报错。
- 后续重新计算或补齐缺失内容时，用 `--supersedes` 指向上一版包生成新版本；新清单记录 version、supersedes 和 changes（新增、移除、替换的候选及依据），原版本保持不变。
- 接收者用 `import-package` 逐件验证：每个产物核对 SHA-256，每条记录重算请求哈希并核对产物引用，通过后才装入本地存储。重复导入同一包幂等，已存在的记录标记 already_present，不产生第二套来源。任何一件缺失或校验失败，整包不得标为 verified_complete。导入的记录可直接被 `run` 复用，从最终结果追溯到实际采用的原实验，无需猜原文件路径。

## 部分交付与补齐

- `freeze --allow-partial` 在部分候选不可交付时仍发布包：健康候选照常打包，不可交付候选写入清单的 candidate_states，状态明确区分为 missing（暂缺：无记录或阶段未结束）与 damaged（损坏：记录或产物校验失败、来源不可信），不会混入健康候选的产物。
- 导入报告为每个候选给出明确状态：complete（installed/already_present）、missing（包内缺记录或缺产物）、damaged（摘要不符）、conflict（本地已在使用不同版本）。缺失或损坏只影响对应候选，健康候选照常导入并可被 `run` 复用；缺失内容永远不会被当作可复用记录。
- 冲突内容不覆盖本地已使用的版本，报告中同时留下本地与 incoming 的产物摘要供核对。
- 接收方可先导入部分包，再导入 `--supersedes` 的补充包：重叠候选保持唯一（already_present），缺口按补齐安装；只有依赖齐备的请求才会被复用，原始机器目录即使已删除也不影响复用。已发布的旧包保持不可变。

## 接收端持久化导入历史

每次 `import-package`（包括重复导入与补发/补齐包）都会在接收存储内追加一条不可变历史（`import-history/imports.jsonl`，落盘 fsync），与 attempts 日志一样重启后完整保留；历史只存在接收者自己的 store 中，查询不依赖原始包目录或任何原始数据目录。

- 每条历史记录包身份：`package_id`、包内容摘要 `package_manifest_digest`、`version`、`supersedes`、导入来源路径（仅展示）。
- 记录本次导入的 `started_at`/`ended_at`、整体结果（`verified_complete`/`completed`/`failed`）及失败原因。
- `requests` 逐个给出请求结果与原因：`installed`、`already_present`（重复导入可追溯但不重复安装或计算）、`missing`、`damaged`、`conflict`，部分包中未随包交付的候选也按其状态记录；另附 `result_counts` 汇总。
- 包完全无法读取（如清单损坏）的失败尝试同样入历史，`package_id` 为 null 并记录异常原因。
- 查询入口：`python -m exp_records.cli import-history --store ./receiver-store [--package <package_id>] [--request <request_hash>]`；Python API 为 `ImportHistory(store).query(package_id=..., request_hash=...)`，二者只读持久化日志。

## 接收端分批补齐计划

`replenish-plan` 在接收端根据本地已验证内容和依赖关系生成最小待请求集合，并把缺口分批安排给发送端补发；整个规划只依赖接收者自己的 store，不需要发送端目录或原始包目录。

- 待请求集合来自一份 wants 清单（JSON 数组，每项含 request_hash、depends_on、priority、size_bytes）；本地已验证的内容直接标为 installable，不再请求。同一内容被多个请求共享（重复条目或共享依赖）只计一次，重复导入与乱序补发不会重复占用已验证容量。
- 分批按优先级（高者优先）与传输预算（`--budget` 每批字节数）安排；`--max-batches` 限制本轮批次数，预算耗尽的请求标记 deferred 留待后续批次；单条超过预算的内容独占一批并标记 over_budget。
- 依赖尚缺失时，上层内容即使已在本地验证也标记 held（保留待用），不会被标为可安装；每个不可安装条目都附 blocking_chain，逐跳解释阻塞原因（未交付、校验失败、依赖未列入请求、循环依赖）。
- 已下载内容校验失败（damaged）时，只有依赖它的请求回到待补齐，独立请求保持 installable；重新补发并通过校验后自动恢复为 installable。
- 计划状态持久化在接收 store 的 `replenish/plan-state.json`（原子写入）；不带 `--wants` 重开时从持久化状态继续生成剩余请求并解释阻塞链，发送端目录不存在也不影响。

    python -m exp_records.cli replenish-plan --store ./receiver-store --wants wants.json --budget 1048576 --max-batches 4
    python -m exp_records.cli replenish-plan --store ./receiver-store   # 从持久化状态恢复

    python -m exp_records.cli freeze-plan --store ./example-store --select <hash>
     python -m exp_records.cli freeze --store ./example-store --output ./packages --select <hash>
     python -m exp_records.cli import-package --store ./receiver-store --package ./packages/<package_id>
 
 ## 测试

    python -m unittest discover -s tests -v
