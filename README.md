# 实验结果复现系统

这是一个零第三方依赖的 Python 独立项目，用内容指纹记录实验输入、参数、运行环境、计算后端和产物。记录成功后不可修改；相同请求复用同一份成功产物，输入、参数、环境或后端身份变化会生成新记录。

## 记录组成

每次运行保存在 `--store` 指定的本地目录：

- `runs/<fingerprint>-<attempt>.json`：完整实验记录，包含输入、参数、环境、后端、输出、产物哈希和状态。
- `artifacts/<record-id>/`：该次成功运行发布的产物文件，记录中保存每个产物的相对路径、SHA-256 和字节数。
- `index.json`：只把请求指纹映射到一次完整、成功的记录。
- `tmp/`：运行暂存区；中断后只留下临时目录，不会生成可复用结果。
- `locks/`：相同请求指纹的文件锁。

失败记录保留在 `runs/` 以便排查，但不会写入 `index.json`，因此不能被复用。多个进程同时提交相同请求时，文件锁保证只执行一次、只发布一份成功产物。重启后通过索引和记录内容重新确认缓存，不依赖进程内存。

## 快速验证

无需安装依赖：

```bash
python -m unittest discover -s tests -v
PYTHONPATH=src python -m experiment_repro.cli --store ./demo_store run sum '[1,2,3]' --parameters '{"scale":2}'
PYTHONPATH=src python -m experiment_repro.cli --store ./demo_store run sum '[1,2,3]' --parameters '{"scale":2}'
```

第二次运行返回 `"reused": true`，且不会产生新的记录号。把数组改为 `[1,2,4]` 会得到新指纹和新记录。

## Python API

```python
from experiment_repro import ExperimentRunner

runner = ExperimentRunner("./experiment_store")
result = runner.run(
    experiment="sum",
    inputs=[1, 2, 3],
    parameters={"scale": 2},
)
print(result.reused, result.record_id, result.record["artifacts"])
```

自定义短计算通过本地后端注册：

```python
from experiment_repro.backends import BackendResult, LocalBackend, Task

def handler(task: Task) -> BackendResult:
    path = task.work_dir / "answer.txt"
    path.write_text(str(task.inputs), encoding="utf-8")
    return BackendResult(outputs={"chars": len(task.inputs)}, artifacts={"answer.txt": path})

backend = LocalBackend()
backend.register("my-experiment", handler)
runner = ExperimentRunner("./experiment_store", backend)
```

## 接入后续计算后端

新后端实现 `ComputeBackend.identity()` 和 `execute()`。`identity()` 必须包含会影响结果含义的名称、版本、镜像、硬件或其他环境信息；其内容参与请求指纹。切换后端不会改写旧记录的含义，而是在确有语义差异时生成新记录。

系统只显式收集 Python 版本、平台和调用方传入的环境声明，不读取用户环境配置清单。
