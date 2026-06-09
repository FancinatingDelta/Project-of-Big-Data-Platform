# Project-of-Big-Data-Platform

[root@worker1 ~]# ls
anaconda-ks.cfg  data  install_python36_worker.sh  openscap_data  portal-x64
[root@worker1 ~]# cd data
[root@worker1 data]# ls
2022-03-20-cloudbed1  2022-03-20-cloudbed2  2022-03-20-cloudbed3

用于完成**日志模式挖掘（Log Pattern Mining）**的代码。

本项目实现并复现论文 **Drain: An Online Log Parsing Approach with Fixed Depth Tree (ICWS 2017)**，
并基于 **Spark** 在集群上对约 **12GB** 的真实微服务日志做分布式模板挖掘与分析。

---

## 一、项目目标与评分要点

**核心任务**：把海量非结构化日志（10GB+）自动归纳为「日志模板 + 出现次数」，并做下游分析。

**评分关键维度**（来自项目说明 PDF）：

1. ⭐ **必须使用集群**：在服务器集群上用 Spark 处理全量数据，而非单机。
2. 算法理解：讲清 Drain 原理，做参数调优分析。
3. 工程质量：处理脏数据（ANSI 码、多行日志、CSV 转义等）。
4. 下游应用：统计、异常检测、可视化。
5. 大数据平台能力：集群扩展性 / 加速比实验。
6. 交付：源代码 + 使用说明、PPT、项目文档；15-30 分钟报告 + Demo。

---

## 二、环境信息

| 项目 | 值 |
| --- | --- |
| 集群节点 | `10.176.62.233`、`10.176.62.234`、`10.176.62.235`（3 节点） |
| 登录账号 | `root` |
| 操作系统 | CentOS 7 |
| 计算框架 | Spark（PySpark） |
| 本地开发 | Python 3.8+、Java 8/11/17、`pip install pyspark` |

> ⚠️ 安全提示：本仓库为 Git 仓库，**请勿把服务器密码明文提交**。建议将凭据写入本地 `.env` 或单独文件并加入 `.gitignore`。
>
> ⚠️ CentOS 7 注意：VSCode 1.98+ 因 glibc 过低无法用 Remote-SSH，可参考
> [vscode-server-centos7](https://github.com/MikeWang000000/vscode-server-centos7) 打补丁，或降级编辑器，或直接用 `ssh` + `spark-submit`。
>
> ⚠️ 网络注意：集群只连局域网，外网资源（数据/依赖）需本地下好再传，或在服务器配 Clash 代理。

---

## 三、数据说明

| 数据 | 位置 | 规模 | 用途 |
| --- | --- | --- | --- |
| 小样本 | `testdata/*.csv` | 6 个文件，各约 1 万行 | **本地开发调试**（已在仓库） |
| 全量数据 | `3-log/2022-03-20-cloudbed{1,2,3}/` | 约 **12GB**，6 个 CSV | **集群上处理**（需上传，不入库） |

- 三套部署环境：`cloudbed1 / cloudbed2 / cloudbed3`。
- 两类日志（由 `log_name` 字段区分）：
  - **Envoy 网关日志**（`*envoy_gateway`）：网络层调用记录，字段规整。
  - **Service 应用日志**（`*service_application`）：服务内部业务/框架日志，自由文本，含 ANSI 颜色码。
- 字段：`log_id, timestamp, cmdb_id, log_name, value`；待解析的是 `value` 列。
- 详细格式见 `testdata/格式说明.md`。

---

## 四、代码结构

```
Project-of-Big-Data-Platform/
├── drain.py                      # Drain 算法单机实现（正确性基准/对照）
├── util.py                       # 工具函数：路由 token、相似度计算
├── preprocess.py                 # Envoy/Service 两类日志的定制正则预处理规则
├── spark_drain.py                # 分布式 Drain：分区并行解析 + 模板相似度合并
├── template_miner.py             # （预留）模板挖掘
├── experiment/
│   ├── run_drain.py              # 单机实验入口（对照）
│   ├── run_spark.py              # ★ Spark 主流程入口（本地/集群一键切换）
│   ├── run_param_sweep.py        # ★ 参数实验：扫描 st/depth，出指标表与图
│   ├── data/test.csv             # 单机算法验证用的微型数据
│   └── result/                   # 输出：模板、指标表、图表
└── testdata/                     # 小样本 + 格式说明
```

---

## 五、完整任务流程（Task Flow）

> 总体路线：**单机算法验证 → 本地 Spark 调通 → 上集群跑全量 → 参数调优 → 下游分析 → 扩展性实验 → 报告与 Demo**。
> 原则：尽早打通「端到端最小闭环」，先用小样本/子集跑通，再逐步加功能、上全量。

### 阶段 0：环境准备与分工

1. **本地环境**：安装 Python 3.8+、JDK、`pip install pyspark pandas matplotlib`。
2. **验证本地 Spark**：
   ```bash
   python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[*]').getOrCreate(); print(s.version); s.stop()"
   ```
3. **连通集群**：`ssh root@10.176.62.233`，确认 Spark 部署方式（Standalone / YARN）、可用节点、能否跑 PySpark。
4. **小组分工**（报告必含）：算法 / Spark 工程 / 预处理 / 下游分析可视化 / 报告 PPT+Demo。

### 阶段 1：理解算法与数据

1. 精读 Drain 论文，画出「5 步流程图」（预处理 → 按长度 → 按前缀 token → 相似度选簇 → 更新树）。
2. 读 `testdata/格式说明.md`，弄清 Envoy / Service 两类 `value` 的格式差异。
3. 跑通单机基准：
   ```bash
   python experiment/run_drain.py
   ```

### 阶段 2：本地 Spark 调通（用小样本）

在 `testdata` 6 个样本上跑通分布式主流程：

```bash
python experiment/run_spark.py \
    --input testdata \
    --output experiment/result/spark \
    --master "local[*]"
```

- 输出：`experiment/result/spark/templates_envoy.txt`、`templates_service.txt`
  （每行格式：`出现次数<TAB>模板`，按次数降序）。
- 人工抽查模板是否合理。

**Windows 本地已知坑（代码已处理）**：
- `NativeIO$Windows.access0` 报错 → 用 Python 枚举文件清单显式传给 Spark（已处理）。
- `Python worker failed to connect back` → 设 `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON`（已处理）。
- CSV 双引号 `""` 转义 → `option("escape", '"')`（已处理）。

### 阶段 3：参数调优实验

扫描相似度阈值 `st` 与树深度 `depth`，分析对模板数量、压缩比、过度合并（通配率）的影响：

```bash
python experiment/run_param_sweep.py \
    --input testdata \
    --output experiment/result/param_sweep \
    --st-list 0.3,0.4,0.5,0.6,0.7 \
    --depth-list 4,5,6
```

- 输出：`param_sweep.csv` + `param_sweep_envoy.png` / `param_sweep_service.png`。
- 结论参考：`st` 越高模板越细、通配率越低；两类日志最优阈值不同（Envoy 偏高、Service 偏中）。

### 阶段 4：上集群跑全量数据（核心）

1. **上传数据**（仅一次，全组共用）：本地 → 集群。
   ```bash
   # 在本地执行；建议先压缩再传，大文件可用 rsync 断点续传
   scp -r 3-log root@10.176.62.233:/root/data/3-log
   ```
   若集群用 HDFS：`hdfs dfs -put 3-log /user/root/3-log`。
2. **上传代码**：
   ```bash
   scp -r Project-of-Big-Data-Platform root@10.176.62.233:/root/
   ```
3. **提交 Spark 作业**（在集群上执行）：
   ```bash
   # Standalone
   spark-submit --master spark://10.176.62.233:7077 \
       experiment/run_spark.py --input /root/data/3-log --output /root/result --master ""
   # 或 YARN
   spark-submit --master yarn --deploy-mode client \
       experiment/run_spark.py --input /root/data/3-log --output /root/result --master ""
   ```
   > 用 `spark-submit --master` 指定时，给脚本参数传 `--master ""` 以免代码内重复覆盖。
4. **取回结果**：`scp root@10.176.62.233:/root/result/* ./experiment/result/cluster/`。

### 阶段 5：下游分析与可视化（计划中）

在模板结果上做：

- 统计：总条数、模板数、压缩比、Top-N 高频模板。
- 异常检测（呼应论文）：Envoy 非 200 状态码模板、罕见模板、模板频率随时间突变。
- 三 cloudbed 对比、错误占比饼图、时间趋势线。

### 阶段 6：集群扩展性 / 加速比实验（核心得分点）

- 固定数据量，变 executor 数 / 节点数（1 vs 2 vs 3），测运行时间 → 画**加速比曲线**。
- 固定资源，变数据量（1GB / 4GB / 12GB），测运行时间 → 验证 Drain 的 **O(n)** 线性。

### 阶段 7：报告、文档与 Demo 收尾

1. `requirements.txt`（pyspark/pandas/matplotlib）与本 README（使用说明）。
2. 项目文档：背景、算法、架构、实验、结论、经验、分工。
3. PPT：完成内容 / 过程 / 数据集 / 经验 / 分工。
4. Demo：现场跑通最小闭环（读数据 → 解析 → 出模板 → 出图）；
   全量耗时长则用子集现场演示 + 提前跑好的全量结果展示。

---

## 六、当前进度

- [x] Drain 单机算法（`drain.py`）
- [x] 两类日志定制预处理（`preprocess.py`）
- [x] 分布式 Drain：分区并行 + 相似度合并（`spark_drain.py`）
- [x] Spark 主流程，本地/集群可切换（`experiment/run_spark.py`），已在小样本跑通
- [x] 参数实验脚本 + 图表（`experiment/run_param_sweep.py`）
- [ ] 集群全量运行
- [ ] 下游分析与可视化
- [ ] 集群扩展性实验
- [ ] 报告 / PPT / Demo

---

## 七、快速开始（本地）

```bash
# 1. 安装依赖
pip install pyspark pandas matplotlib

# 2. 在小样本上跑通模板挖掘
python experiment/run_spark.py --input testdata --output experiment/result/spark --master "local[*]"

# 3. 参数实验
python experiment/run_param_sweep.py --input testdata --output experiment/result/param_sweep
```
