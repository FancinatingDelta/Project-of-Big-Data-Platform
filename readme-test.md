# Project-of-Big-Data-Platform 运行指南

用于完成**日志模式挖掘（Log Pattern Mining）**的代码。

本项目实现并复现论文 **Drain: An Online Log Parsing Approach with Fixed Depth Tree (ICWS 2017)**，
并基于 **Spark** 在集群上对约 **12GB** 的真实微服务日志做分布式模板挖掘与分析。

---

## 环境信息

| 项目 | 值 |
| --- | --- |
| 集群节点 | `master`(10.176.62.233)、`worker1`(10.176.62.234)、`worker2`(10.176.62.235) |
| 操作系统 | CentOS 7 |
| Python | 3.8.18（命令 `python3`） |
| 计算框架 | Spark 3.x（PySpark on YARN） |
| 本地开发 | Python 3.8+、Java 8+、`pip install pyspark` |

---

## 数据说明

| 数据 | 位置 | 规模 |
| --- | --- | --- |
| 小样本 | `testdata/*.csv` | 6 个文件，各约 1 万行 |
| 全量数据 | `/root/data/2022-03-20-cloudbed{1,2,3}/` | 约 **12GB**，6 个 CSV |

每个 `cloudbed` 目录下有两个文件：
- `log_filebeat-testbed-log-envoy.csv` — Envoy 网关日志
- `log_filebeat-testbed-log-service.csv` — Service 应用日志

CSV 固定 5 列：`log_id, timestamp, cmdb_id, log_name, value`，Drain 解析的是 `value` 列。

---

## 代码结构

```
Project-of-Big-Data-Platform/
├── drain.py                      # Drain 算法单机实现
├── util.py                       # 工具函数：路由 token、相似度计算
├── preprocess.py                 # Envoy/Service 两类日志的定制正则预处理规则
├── spark_drain.py                # 分布式 Drain：两阶段 MapReduce + 参数树构建
├── param_tree.py                 # 参数分布树：为每个 <*> 位置构建层次分布
├── experiment/
│   ├── run_drain.py              # 单机实验入口
│   ├── run_spark.py              # ★ Spark 主流程入口
│   ├── run_param_sweep.py        # 参数实验
│   └── result/                   # 输出
├── testdata/                     # 小样本
└── result/                       # 全量结果
```

---

## 运行流程

### 整体数据流

```
CSV 文件
  │
  ▼
read_logs() ── 读取所有 CSV，保留 (log_name, value)
  │
  ├── filter("envoy_gateway") ── Envoy 分支
  │     ├── mine_templates() ─────── 两阶段 MapReduce ──► templates_envoy.txt
  │     └── build_param_trees() ──── 二次遍历匹配 ────► templates_envoy_tree.txt
  │
  └── filter("service_application") ── Service 分支（同上）
        ├── mine_templates() ───────► templates_service.txt
        └── build_param_trees() ────► templates_service_tree.txt
```

### 阶段 1：分区独立 Drain 解析（Map）

`spark_drain.py: parse_partition()` — 每个 Spark 分区独立构建一棵 Drain 树，串行解析该分区日志，产出局部模板 `(template, count)` 列表。

Drain 树工作原理：
1. **预处理** — 正则替换 IP、端口、Trace ID、数字等动态内容
2. **按长度路由** — 树的第一层按日志 token 数量分叉
3. **按前缀路由** — 后续层按首 token 值路由，相似日志落入同一叶子
4. **相似度匹配** — 在叶子层的 Cluster 列表中找最相似模板；超过阈值则合并（差异位置替换为 `<*>`），否则新建模板

### 阶段 2：全局模板合并（Reduce）

`spark_drain.py: merge_templates()` — 收集所有分区的局部模板，按 token 长度分桶，桶内按相同相似度阈值合并去重，得到全局模板列表，按出现次数降序。

### 阶段 3：参数分布树构建

`spark_drain.py: build_param_trees()` — 拿到最终模板后，**二次遍历日志**：

1. 构建查找表 `{token长度: [(模板索引, token列表), ...]}`，Broadcast 到所有 executor
2. 每个分区内：预处理日志 → 分词 → 按长度+相似度匹配最佳模板 → 提取 `<*>` 位置的参数值 → 生长到局部 `dict[模板索引 → ParamTree]`
3. Collect 所有分区的局部树到 Driver，按模板索引合并
4. `param_tree.py: render_param_tree()` 渲染为树状文本

### 输出格式

**扁平模板**（`.txt`）：每行 `次数<TAB>模板字符串`，按次数降序。

**参数分布树**（`_tree.txt`）：
- 每层最多展示 5 个分支（按计数降序）
- 超出部分折叠为 `└─ ... 共 N 种参数组合（剩余 M 条）`
- 无 `<*>` 的模板仅输出 `[次数] 模板文本`
- 用 `├─`/`└─`/`│` 绘制树状连接线

示例：
```
[309] severity: info, message: <*> <*> <*>
  ├─ received (121)
  │  └─ conversion (121)
  │     └─ request (121)
  ├─ conversion (103)
  │  └─ request (103)
  │     └─ successful (103)
  └─ ... 共 6 种参数组合（剩余 1 条）
```

---

## 本地运行

```bash
# 安装依赖
pip install pyspark pandas matplotlib

# 小样本快速测试（1000 条）
python3 experiment/run_spark.py \
    --input testdata \
    --output experiment/result/spark \
    --master "local[*]" \
    --limit 1000

# 小样本全量
python3 experiment/run_spark.py \
    --input testdata \
    --output experiment/result/spark \
    --master "local[*]"

# 参数实验
python3 experiment/run_param_sweep.py \
    --input testdata \
    --output experiment/result/param_sweep
```

### 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--input` | `testdata` | 输入目录（递归读取 .csv） |
| `--output` | `experiment/result/spark` | 输出目录 |
| `--master` | `local[*]` | Spark master；集群提交时传 `""` |
| `--max-depth` | 5 | Drain 树最大深度 |
| `--max-children` | 100 | 每层最大子节点数 |
| `--st` | 0.5 | 相似度阈值 |
| `--limit` | 0 | 仅取前 N 条调试，0=全部 |

---

## 集群运行（YARN）

### 1. 部署代码

```bash
# 上传所有 .py 文件到 master 节点
scp drain.py util.py preprocess.py spark_drain.py param_tree.py \
    root@10.176.62.233:/root/project-test/
scp experiment/run_spark.py \
    root@10.176.62.233:/root/project-test/experiment/
```

### 2. 打包模块（YARN executor 需要）

```bash
cd /root/project-test

python3 -c "
import zipfile
with zipfile.ZipFile('drain_modules.zip', 'w') as z:
    for f in ['drain.py', 'util.py', 'preprocess.py', 'spark_drain.py', 'param_tree.py']:
        z.write(f)
print('OK')
"
```

### 3. 提交 Spark 作业

```bash
cd /root/project-test

spark-submit --master yarn --deploy-mode client \
    --py-files drain_modules.zip \
    --executor-memory 6g \
    --executor-cores 2 \
    --num-executors 2 \
    --conf spark.network.timeout=600s \
    --conf spark.executor.heartbeatInterval=60s \
    experiment/run_spark.py \
    --input /root/data \
    --output /root/result/full_tree \
    --master ""
```

| 参数 | 作用 |
|------|------|
| `--py-files drain_modules.zip` | **必需**，把 Python 模块分发到所有 YARN executor |
| `--master ""` | 让 `spark-submit --master yarn` 接管，代码内不覆盖 |
| `--input /root/data` | 各节点本地数据路径（每节点需有相同副本） |
| `spark.network.timeout=600s` | 大数据分区处理超时保护 |

### 4. 监控

```bash
# 看作业状态
yarn application -list

# 看节点资源
yarn node -list -all

# Spark UI
http://master:4040
```

### 5. 取回结果

```bash
scp root@10.176.62.233:/root/result/full_tree/* ./experiment/result/full_tree/
```

---

## 常见问题

| 现象 | 原因 | 解决 |
|------|------|------|
| `SyntaxError: Non-ASCII character` | `python` 指向 2.7 | 用 `python3` |
| `No module named 'spark_drain'` | executor 没有 .py 文件 | `--py-files drain_modules.zip` |
| `Path does not exist: hdfs://...` | 本地路径被当 HDFS | 代码自动检测本地文件加 `file://` |
| `Executor heartbeat timed out` | 大数据分区超 120s | `--conf spark.network.timeout=600s` |
| 作业一直 ACCEPTED | 资源不足/僵尸作业 | `yarn application -kill <id>` 清理 |
| `zip: command not found` | CentOS 无 zip | `python3 -c "import zipfile; ..."` 打包 |
| `Connection refused: 7077` | Spark Standalone 未启动 | 用 YARN 模式 |

---

## 当前进度

- [x] Drain 单机算法（`drain.py`）
- [x] 两类日志定制预处理（`preprocess.py`）
- [x] 分布式 Drain：分区并行 + 相似度合并（`spark_drain.py`）
- [x] Spark 主流程，本地/集群可切换（`experiment/run_spark.py`）
- [x] 参数实验脚本 + 图表（`experiment/run_param_sweep.py`）
- [x] 树状输出：参数分布树（`param_tree.py`）
- [x] **集群全量运行**（~3000 万条日志，分布式 Drain + 参数树）
- [ ] 下游分析与可视化
- [ ] 集群扩展性实验
- [ ] 报告 / PPT / Demo
