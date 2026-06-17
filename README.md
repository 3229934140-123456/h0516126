# CI/CD 流水线执行引擎

一个模块化、可扩展的 CI/CD 流水线执行引擎，完整实现了流水线定义、依赖调度、隔离执行、产物传递、实时日志、条件执行与失败处理等核心功能。

## 模块架构

```
cicd_engine/
├── models/
│   └── pipeline.py           # 核心数据模型：流水线、阶段、步骤、依赖图
├── parser/
│   └── pipeline_parser.py    # 流水线定义解析器（YAML/JSON）
├── scheduler/
│   └── scheduler.py          # 调度执行引擎（依赖图、并行、重试）
├── executor/
│   └── executor.py           # 执行环境（子进程/容器隔离）
├── artifacts/
│   └── artifact_manager.py   # 产物管理（上传/下载/传递）
├── logging/
│   └── log_stream.py         # 日志流（实时流式、持久化、订阅）
├── state/
│   └── state_manager.py      # 状态管理（聚合、持久化、查询）
└── engine.py                 # 引擎入口（API 统一封装）
```

---

## 一、流水线定义：阶段与步骤的依赖图

### 1.1 数据模型

在 [models/pipeline.py](file:///d:/trae-bz/TraeProjects/126/cicd_engine/models/pipeline.py) 中定义了流水线的核心数据结构：

- **Pipeline**：顶层流水线对象，包含多个 Stage（阶段）
- **Stage**：阶段，是步骤的逻辑分组，可声明对其他 Stage 的依赖
- **Step**：步骤，最小执行单元，可声明对其他 Step 的依赖

### 1.2 依赖图 DependencyGraph

`DependencyGraph` 类将流水线的阶段和步骤构建为有向无环图（DAG）：

```python
# 节点格式：
stage:{stage_name}        # 阶段节点
step:{stage}:{step}       # 步骤节点

# 边的来源：
# 1. Stage.depends_on      → stage_A → stage_B
# 2. Step.depends_on        → step_X → step_Y
#    支持跨阶段引用："OtherStage:StepName"
```

核心方法：
- `topological_sort()`：拓扑排序，得到合法的执行顺序
- `detect_cycles()`：循环依赖检测
- `get_dependencies(node)`：获取节点的直接依赖
- `get_ready_nodes(completed)`：计算当前可调度的就绪节点

### 1.3 流水线定义示例

支持 YAML 和 JSON 两种格式（见 [parser/pipeline_parser.py](file:///d:/trae-bz/TraeProjects/126/cicd_engine/parser/pipeline_parser.py)）：

```yaml
name: my-pipeline
variables: [{key: VERSION, value: "1.0"}]
stages:
  - name: build
    steps:
      - name: compile
        script: ["npm install", "npm run build"]
        artifacts_out: [{name: dist, source_path: dist}]
  - name: test
    depends_on: [build]          # 阶段依赖
    steps:
      - name: unit               # 无 step 依赖则 build 完成后即可运行
        script: ["npm test"]
        artifacts_in: [dist]     # 消费上游产物
  - name: deploy
    depends_on: [test]
    steps:
      - name: canary
        script: ["kubectl apply ..."]
      - name: production
        depends_on: [canary]     # 步骤内依赖
        script: ["kubectl rollout ..."]
        condition:               # 条件执行
          variable: DEPLOY_ENV
          operator: eq
          value: production
```

---

## 二、调度执行：依赖驱动的并行调度

调度核心在 [scheduler/scheduler.py](file:///d:/trae-bz/TraeProjects/126/cicd_engine/scheduler/scheduler.py) 的 `PipelineScheduler` 类。

### 2.1 调度循环算法

```
while 还有未处理的节点:
    1. 收集已完成的 futures，更新状态
    2. 聚合阶段状态（所有 Step 完成 → Stage 完成）
    3. 检查失败策略，决定是否中止
    4. 启动依赖满足的 Stage（检查 processed_stages 依赖）
    5. 调度就绪 Step（所属 Stage 已启动 + 所有 Step 依赖完成）
    6. 线程池并行执行 Step
    7. 死锁检测与超时处理
```

### 2.2 并行执行机制

- 使用 `concurrent.futures.ThreadPoolExecutor` 线程池
- 同一 Stage 内无依赖的 Step 自动并行
- 跨 Stage 的 Step 只要依赖满足也可并行
- `max_workers` 参数控制并发度

### 2.3 失败处理策略 (`FailureStrategy`)

每个 Pipeline / Stage / Step 可指定独立的失败策略：

| 策略 | 行为 |
|------|------|
| `ABORT` | 失败立即中止，后续步骤标记为 SKIPPED（默认） |
| `CONTINUE` | 失败后继续执行其他不相关的步骤 |
| `RETRY` | 配合 `retries` 参数重试指定次数 |
| `ROLLBACK` | 保留 ABORT 语义，可扩展触发回滚 |

另外 Step 的 `allow_failure: true` 可让该步骤失败被视为成功。

### 2.4 重试机制

```
Step.retries = N → 最多执行 N+1 次
每次失败后等待 retry_delay 秒
成功立即跳出循环
```

### 2.5 条件执行 (`Condition`)

在 Stage 或 Step 上可配置 condition，支持多种形式：

**简单比较：**
```yaml
condition:
  variable: BUILD_TYPE
  operator: eq           # eq/ne/gt/lt/gte/lte/in/not_in/exists
  value: release
```

**复合条件：**
```yaml
condition:
  operator: and          # and/or/not
  conditions:
    - {variable: A, operator: eq, value: 1}
    - {variable: B, operator: exists}
```

**表达式求值：**
```yaml
condition: "success and progress > 50"
```

条件求值的上下文变量：
- 所有 Pipeline/Stage/Step 变量
- `steps.{stage}_{step}_success`：前序步骤状态
- `success` / `failed`：流水线整体状态
- `progress` / `duration`：执行进度

---

## 三、执行环境：隔离的命令运行

### 3.1 SubprocessEnvironment（子进程隔离）

见 [executor/executor.py](file:///d:/trae-bz/TraeProjects/126/cicd_engine/executor/executor.py)。

核心特性：
- **工作目录隔离**：每个 Step 使用独立目录 `workspace/{pipeline_id}/{stage}/{step}`
- **环境变量隔离**：通过 `subprocess.Popen(env=...)` 传参，不污染父进程
- **命令执行**：通过 shell 执行（bash/cmd/powershell），支持变量替换 `$VAR` 或 `${VAR}`
- **实时输出流**：独立线程分别读取 stdout/stderr，逐行流式推送到日志模块
- **超时控制**：`process.wait(timeout)` 超时后强制终止进程树
- **进程终止**：
  - Linux：`killpg(SIGTERM → SIGKILL)` 终止整个进程组
  - Windows：`taskkill /F /T /PID` 递归终止子进程
- **输出变量解析**：识别脚本中的 `::set-output name=X::value` 语法

### 3.2 DockerEnvironment（容器隔离，可选）

当 `Step.image` 指定镜像且系统安装了 Docker 时自动启用：
```
执行流程：
  1. docker pull {image}
  2. 挂载工作目录 -v {workdir}:/workspace
  3. 注入环境变量 -e KEY=VALUE
  4. docker run --rm image sh -c "commands"
```
不可用时自动降级为 SubprocessEnvironment。

### 3.3 环境变量注入链

每个 Step 的执行环境变量按优先级合并：
```
系统 PATH/HOME  <--  低优先级
    ↓
Pipeline.variables
    ↓
Stage.variables
    ↓
Step.variables
    ↓
Step.environment（配置文件中直接写的字典）
    ↓
Step 级保留变量（PIPELINE_ID/STAGE_NAME/STEP_NAME/CI=true...）
    ↓
前序步骤 ::set-env 导出的变量   <-- 高优先级
```

---

## 四、产物管理：步骤间的数据传递

核心在 [artifacts/artifact_manager.py](file:///d:/trae-bz/TraeProjects/126/cicd_engine/artifacts/artifact_manager.py)。

### 4.1 数据结构

- **Artifact**：声明式产物定义（名称、源路径、保留天数、是否压缩）
- **StoredArtifact**：已存储产物的元数据（存储键、大小、MD5、创建时间）

### 4.2 存储后端

| 后端 | 适用场景 | 特性 |
|------|---------|------|
| `FileSystemStore` | 持久化、单机 | 存本地文件系统，支持压缩（tar.gz/gzip），MD5 校验 |
| `MemoryStore` | 测试、临时 | 内存存储，进程结束丢失，速度快 |

可扩展实现 S3/OSS/MinIO 等对象存储（实现 `ArtifactStore` 接口即可）。

### 4.3 产物生命周期

**发布（publish）**：
```
Step 完成后
  → 对 artifacts_out 列表逐项处理
      → 如果是目录：打包为 .tar.gz（或未压缩 .tar）
      → 如果是文件：.gz 压缩（或原样复制）
      → 计算 MD5 + 大小
      → 存储到后端（key: {pid}/{stage}/{step}/{artifact_id}）
      → 写入元数据 JSON
```

**恢复（restore）**：
```
Step 执行前
  → 对 artifacts_in 列表逐项查找
      → 先按名称精确匹配（当前 pipeline 内）
      → 再按模式通配查找
      → 下载解压到当前 Step 的工作目录/{artifact_name}/ 下
```

**自动过期清理**：`retention_days` 到期后 `cleanup_expired()` 自动删除。

---

## 五、日志流：实时流式输出

核心在 [logging/log_stream.py](file:///d:/trae-bz/TraeProjects/126/cicd_engine/logging/log_stream.py)。

### 5.1 分层架构

```
子进程 stdout/stderr
      ↓ (逐行读取)
LogStream.log(level, message, context...)
      ↓
  ┌───────────────┬───────────────────┬──────────────────┐
  │ 日志存储层    │  订阅推送层       │  控制台输出      │
  │ (多后端)      │  (pub/sub)        │  (可选)          │
  └───────────────┴───────────────────┴──────────────────┘
```

### 5.2 LogEntry 字段

每条日志包含完整上下文：
```python
{
  "log_id": "uuid",
  "timestamp": 1718...,
  "level": "info|stdout|stderr|warn|error...",
  "pipeline_id": "...",
  "stage_name": "build",
  "step_name": "compile",
  "command_index": 0,      # 第几条命令
  "message": "...",
  "metadata": {}
}
```

### 5.3 存储后端

| 后端 | 特点 |
|------|------|
| `InMemoryLogStore` | deque 环形缓冲（默认 10w 条） |
| `FileLogStore` | JSON Lines 格式，按 pipeline 分目录，支持按 step 切分文件 |

### 5.4 订阅机制（Subscriber）

```python
# 回调式订阅
stream.subscribe(CallbackSubscriber(
    callback=lambda entry: send_to_websocket(entry),
    min_level=LogLevel.DEBUG
))

# 用于 WebSocket 实时推送前端
```

### 5.5 日志查询接口

```python
logs = engine.get_logs(
    pipeline_id="xxx",
    stage_name="build",
    step_name="compile",
    min_level=LogLevel.ERROR,
    limit=1000,
)

tail = engine.tail_logs(pipeline_id="xxx", lines=200)
```

---

## 六、状态聚合与持久化

核心在 [state/state_manager.py](file:///d:/trae-bz/TraeProjects/126/cicd_engine/state/state_manager.py)。

### 6.1 状态枚举

**StepStatus**（8 种）：`PENDING/WAITING/RUNNING/SUCCESS/FAILED/SKIPPED/CANCELLED/TIMEOUT`

**PipelineStatus**（7 种）：`PENDING/RUNNING/PAUSED/SUCCESS/FAILED/CANCELLED/PARTIAL_SUCCESS`

### 6.2 聚合规则（StatusAggregator）

自底向上的状态聚合：

```
多个 Step 状态  ──aggregate──▶  Stage 状态
    │
    ├ 任一 RUNNING  → RUNNING
    ├ 全部 SKIPPED  → SKIPPED
    ├ 全部 SUCCESS+SKIPPED → SUCCESS
    ├ 有 FAILED 又有 SUCCESS → SUCCESS（可被 allow_failure 抵消）
    ├ 有 FAILED 无 SUCCESS → FAILED
    └ 其余 → PENDING

多个 Stage 状态 ──aggregate──▶  Pipeline 状态
    │
    ├ 任一 RUNNING/WAITING → RUNNING
    ├ 全部 SUCCESS/SKIPPED → SUCCESS
    ├ 有 FAILED + SUCCESS → PARTIAL_SUCCESS
    ├ 有 FAILED 无 SUCCESS → FAILED
    ├ 有 CANCELLED → CANCELLED
    └ 其余 → PENDING
```

### 6.3 PipelineExecutionState

内存中的完整执行状态，包含：
- 每个 Stage/Step 的实时状态、开始/结束时间
- 变量快照（variables + output_variables）
- 进度百分比（`progress` 字段）
- 耗时计算（`duration`）

持久化格式为 JSON（`FileStateStore`），可随时加载恢复。

### 6.4 持久化策略

- `auto_persist=True`：每次状态变更立即写盘（原子写：tmp + rename）
- 手动调用 `state_manager.persist()` 批量刷盘
- 内存态 + 磁盘态双写，查询优先走内存

---

## 七、引擎入口与使用示例

统一入口在 [engine.py](file:///d:/trae-bz/TraeProjects/126/cicd_engine/engine.py) 的 `PipelineEngine` 类。

### 7.1 快速开始

```python
from cicd_engine import PipelineEngine
from cicd_engine.logging.log_stream import LogLevel

with PipelineEngine.in_memory(max_workers=4) as engine:
    # 1. 验证定义
    ok, errors, warnings = engine.validate("examples/example_pipeline.yaml")
    if not ok:
        print("Errors:", errors)
        exit(1)

    # 2. 查看执行计划（Dry Run）
    plan = engine.dry_run("examples/example_pipeline.yaml")
    for i, batch in enumerate(plan["parallel_groups"]):
        print(f"Batch {i+1}: {batch}")

    # 3. 执行流水线（可传入变量覆盖）
    result = engine.run_file(
        "examples/example_pipeline.yaml",
        variables={"DEPLOY_ENV": "staging"},
    )

    # 4. 查看结果
    print(f"Status: {result.status.value}")
    print(f"Duration: {result.duration:.2f}s")
    for sr in result.stage_results:
        print(f"  {sr.stage_name}: {sr.status.value}")
```

### 7.2 完整功能 API

```python
# 事件监听
def on_event(evt):
    if evt.type == SchedulerEventType.STEP_FAILED:
        alert(evt.message)
engine.on_event(on_event)

# 日志订阅
def on_log(entry):
    ws.send(entry.to_json())
engine.on_log(on_log, min_level=LogLevel.STDOUT)

# 运行时控制
engine.pause(pid)       # 暂停
engine.resume(pid)      # 恢复
engine.cancel(pid)      # 取消

# 历史查询
states = engine.list_pipelines(limit=50)
logs = engine.get_logs(pid, step_name="compile")
result = engine.get_pipeline_result(pid)
```

### 7.3 运行示例

```bash
pip install pyyaml          # YAML 支持（可选，JSON 不需要）
python run_example.py       # 运行内置示例
```

---

## 八、关键技术要点总结

| 问题 | 解决方案 | 位置 |
|------|---------|------|
| 循环依赖 | DFS 三色标记 + 拓扑排序计数校验 | `models/pipeline.py::DependencyGraph` |
| 并行调度 | 线程池 + 依赖就绪检查 + stage/step 两阶段启动 | `scheduler/scheduler.py::_run_schedule_loop` |
| Stage-Step 死锁 | Step 对所属 Stage 不设硬依赖，仅要求 Stage 已启动（started） | `models/pipeline.py::_build_graph` |
| 命令实时输出 | 双线程分别读 stdout/stderr，队列+回调流式推送 | `executor/executor.py::SubprocessEnvironment.execute` |
| 变量作用域 | 分层合并链 + `::set-output`/`::set-env` 语法解析 | `executor/executor.py::OutputVariableParser` + `_build_env_vars` |
| 状态一致性 | 每次状态变更触发聚合，支持部分成功（PARTIAL_SUCCESS） | `state/state_manager.py::aggregate_from_steps` |
| 原子持久化 | JSON 临时文件 + `os.replace()` 原子替换 | `state/state_manager.py::FileStateStore.save` |
| 产物寻址 | 三级索引（pipeline 级、stage-step 级、名称级）+ 最近匹配 | `artifacts/artifact_manager.py::_resolve_name` |
| 进程树清理 | Linux killpg + Windows taskkill /T，双重保险 | `executor/executor.py::_kill_process` |

---

## 九、可扩展点

1. **存储后端**：实现 `ArtifactStore` 接口对接 S3/OSS
2. **日志后端**：实现 `LogStore` 接口对接 ELK/Loki
3. **状态后端**：实现 `StateStore` 接口对接 MySQL/Redis
4. **执行环境**：继承 `ExecutionEnvironment` 对接 K8s Job
5. **通知插件**：通过事件订阅（`on_event`）对接企业微信/钉钉/邮件
6. **鉴权中间件**：在 engine 层封装 RBAC 鉴权与审计日志
