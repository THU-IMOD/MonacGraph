# MonacGraph
A graph database system based on the TinkerPop framework, supporting second-order logic.
The storage backend is our community structure based graph storage system, i.e., LSM-Community.

## Demo Video

Watch the full demonstration of MonacGraph:  
https://www.youtube.com/watch?v=Eezdq9tzbJE

## MonacGraph Build & Run Guide

This guide explains how to compile the native LSM storage engine, build the Java application, and run the server/clients.

Prerequisites: JDK 17, Maven, Rust/Cargo, Node.js/npm, and CMake with a C++17
compiler. Set `JAVA_HOME` to the JDK before configuring the C++ JNI library.

### 1. Build Native Storage Engine (Rust)

First, compile the Rust project and copy the native library to the Java resource directory.

```bash
# 1. Enter the Rust sub-project directory
cd lsm-community

# 2. Compile the optimized release version
cargo build -p lsm-community-java --release

# 3. Copy the native library to Java resources (run ONLY the command for your OS)
# Windows
mkdir -p ../src/main/resources/storage/windows/
cp ./target/release/lsm_community_java.dll ../src/main/resources/storage/windows/
# Linux
mkdir -p ../src/main/resources/storage/linux/
cp ./target/release/liblsm_community_java.so ../src/main/resources/storage/linux/
# macOS
mkdir -p ../src/main/resources/storage/macos/
cp ./target/release/liblsm_community_java.dylib ../src/main/resources/storage/macos/

# 4. Return to the project root directory
cd ..
```

### 2. Build Subgraph Matching Engine (C++, Optional)

> This step is only required if you need the **second-order logic subgraph matching** feature
> (`g.Subgraph().addV(...).addE(...).forall(...).filter(...).execute()`).
> Skip this step if you only use community-based or vertex subset queries.

The subgraph matching engine is located in the `subgraph/` directory.
Make sure CMake (3.16+), a C++17 compiler, and your JDK are installed before proceeding.

```bash
# 1. Enter the subgraph sub-project directory
cd subgraph

# 2. Configure and build the shared library
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release

# 3. Copy the native library to Java resources (run ONLY the command for your OS)
# Windows
cp build/Release/subgraph_matching.dll ../src/main/resources/storage/windows/
# Linux
cp build/libsubgraph_matching.so ../src/main/resources/storage/linux/
# macOS
cp build/libsubgraph_matching.dylib ../src/main/resources/storage/macos/

# 4. Return to the project root directory
cd ..
```

### 3. Build Java Application
Compile the Java project and package it into a JAR file.

```bash
mvn clean package
```

### 4. Running the Server
Start the Gremlin Server. Ensure the server is fully started (wait for the port binding log) before running any clients.

```bash
java -cp target/Gremmunity-1.0-SNAPSHOT.jar db.monacgraph.app.MonacGraphServer
```

### 5. Running Web Client
Open a new terminal window to run the web-based visualization client.

**Prerequisites**: Ensure Node.js (v16+) and npm are installed on your system.

Run the Web Client:

```bash
# 1. Navigate to the demo (web client) subdirectory
cd web-client

# 2. Install all required Node.js dependencies for the web client
npm install

# 3. Start the development server (runs on http://localhost:5173 by default)
npm run dev
```

**Access the Web Interface**:
After the server starts successfully, you will see the local access URL in the terminal: `http://localhost:5173/`.

**Method 1 (Click the Link)**:
Hold `Ctrl` (Windows/Linux) or `Cmd` (macOS) and click the URL directly in the terminal to open it in your default browser.

**Method 2 (Command Line Instruction)**:
Run the following command in a new terminal to open the URL automatically:

```bash
# Open the web interface automatically (run ONLY the command for your OS)
# Windows (PowerShell)
start http://localhost:5173/
# Linux
xdg-open http://localhost:5173/
# macOS
open http://localhost:5173/
```


### 6. Custom Usage
You can also build your own client applications. Since this project is compatible with Apache TinkerPop, you can use the Gremlin query language to perform graph traversals and queries against the server.

### Local verification and sample data

Keep both the Java server (started from the repository root) and the Vite server
running. Open `http://localhost:5173`, then connect to `localhost:8182`.
The upload service health check is `http://localhost:8284/api/health`.

- **Create Test Data** creates four people and three directed edges.
- **Upload Graph** accepts a `.graph` topology, plus optional CSV/JSON property
  files. For the bundled larger example, use `data/example.graph`,
  `data/exampleVertexProperty.csv`, and `data/exampleEdgeProperty.csv` with a
  new graph name. The result has 13 vertices and 20 edges.
- Use **Examples** for Vset, second-order logic, WCC, SCC, Community, and BFS.
- **Clear Graph** calls `graph.clear()`, resets the current database, and keeps
  its previous topology and storage under `workspace/backups/`. Native
  per-vertex deletion is not implemented; do not use `g.V().drop()` to clear it.

On restart the server opens `my_database` by default. To reopen an uploaded
graph, run `graph.reload('your_graph_name')` in the query editor and refresh.
Load its vertex/edge property files again if needed.


## 可选存储后端：Bw-Graph / LSM-Community

`CommunityGraph` 通过 `GraphStorage` 选择后端，原 Gremlin、Vset、Subgraph、属性接口保持一致。
缺省是 LSM-Community。`conf/lsm-community.properties` 使用 `storage.backend=lsm-community`；
`conf/bw-graph.properties` 使用 `storage.backend=bw-graph`，并配置：

```properties
gremlin.graph=db.monacgraph.community.CommunityGraph
storage.backend=bw-graph
db.name=my_database
storage.path=data
bwgraph.storage.root=workspace/bw-graph
bwgraph.import.on-create=true
# 可选：本机编译的 JNI 库绝对路径
# bwgraph.library.path=/absolute/path/libbwgraph_jni.dylib
```

Bw-Graph 每个图使用 `<bwgraph.storage.root>/<db.name>/` 独立目录；LSM 继续使用 `workspace/<db.name>/`。
二者磁盘格式不互通，不会直接迁移已有 Rust 数据库。Bw 新库可从 `data/<name>.graph` 的
`t/v/e` 文件导入（所有顶点先建立，再插入有向边），也可以从空图逐点、逐边构建。
文件 ID 经外部 ID 映射，CSV/JSON 属性仍按文件 ID 匹配。文件中的 community 字段不作为 Bw 的布局指令。
`reload(name)` 保留后端配置并打开现有图；`clear()` 备份本后端数据库再创建空库。
HTTP 上传显式替换目标图，会备份旧数据库；激活失败返回错误，不会报告成功或偷偷切回 LSM。
上传不会重写共享 Groovy 初始化脚本；要在重启后选择上传的图，请修改所选 properties 中的 `db.name`。
切图、清空、上传属于维护操作，运行时应暂停其他查询；它们不是与任意遍历并发的事务。

### 构建、验证和启动

存储引擎位于 `bw-cloud-graph/` 子模块，构建脚本默认使用该目录：

| MonacGraph 分支 | 子模块跟踪分支 | 启动命令 |
| --- | --- | --- |
| `bw-graph` | `bw-graph-java` | `bash scripts/bwgraph.sh start` |
| `bw-cloud-graph` | `bw-cloud-graph-java` | `bash scripts/bwcloud.sh start` |

普通 `git submodule update --init --recursive` 使用父仓库暂存/提交的固定版本；
`branch` 配置只控制显式的 `git submodule update --remote`，不会自动拉取更新。
两个版本尚未提交时分别保留在独立 worktree 中，可用 `git worktree list` 查看。


需要 Java 17+、Maven，以及按 Bw-Graph 仓库说明安装的 C++ 依赖。以下从 MonacGraph 根目录运行：

```bash
export JAVA_HOME="$(/usr/libexec/java_home -v 17)" # 或手动指定已安装 JDK
git submodule update --init --recursive
export BWGRAPH_SOURCE="$PWD/bw-cloud-graph" # 默认值，可省略
bash scripts/bwgraph.sh build
bash scripts/bwgraph.sh start
# 选择原后端：
bash scripts/bwgraph.sh start gremlin-server.yaml
```

build 编译 Bw JNI、LSM Rust JNI 和子图匹配 JNI，校验 Bw Java JNI 声明一致，复制当前平台库至资源目录并执行 Maven 打包与全部集成测试。还需 Rust/Cargo 和 CMake。
库文件是构建产物，不应提交。已有本地构建可直接验证：

```bash
export BWGRAPH_JNI_LIBRARY="$BWGRAPH_SOURCE/build-monac-jni/lib/libbwgraph_jni.dylib" # Linux 用 .so
bash scripts/bwgraph.sh test
mvn package -Dbwgraph.integration=true -Dbwgraph.jni.path="$BWGRAPH_JNI_LIBRARY"
bash scripts/bwgraph.sh start
```

启动器预载 JDK `libjsig`，避免 Folly 的 SIGPIPE 初始化覆盖 HotSpot 信号处理器。
Maven 集成测试也预载它并使用 `-Xcheck:jni`；原生 Rust stdout 使用 Surefire TCP 通道，不混入测试协议。
JNI 库仍依赖编译环境的共享库，复制 dylib/so 不等于跨机器独立部署。
`-Dmonac.upload.port=...` 可调整上传端口（默认 8284）；Gremlin 监听设置在所选 YAML 中。

### 能力边界

Bw 支持外部 UUID/字符串 ID、属性、双向查询有向边、顶点级联删除、正常关闭重开，以及 BFS/WCC/SCC。
`Community()` 在当前图的简单无向投影上运行 Louvain；LSM 的此接口返回已存储社区，两者语义有区别。
并行边不支持；GraphComputer、Java 多语句事务、崩溃恢复和原生远程 RPC 不在此接入范围。
编译 `--txn` 不代表 Java 属性/ID 映射具有跨操作快照事务。LSM 原有 native `removeVertex` 尚无实现，
本次未宣称补齐其顶点删除能力；Bw 的删除及关联边清理有单独测试。
导入和社区分析会使用临时图结构，尚未作大图性能优化。

验证入口：`StorageBackendTest` 覆盖两后端 CRUD、属性、分析、重开及 Bw 的导入/并发/隔离；
`StorageServerTest` 启动真实 Gremlin/HTTP 服务，覆盖远程遍历、上传替换、清空和后端保持。
不设置 `bwgraph.integration=true` 时这些测试会跳过，因此普通 `mvn test` 不是双后端验证结果。
本次为 Apple Silicon macOS 验证，Linux/Windows 部署尚未验证。

严格 JNI 检查下，本机 Java 17 + Netty 的池化 zlib 编解码可在不加载任何图数据库库的独立程序中复现错误。
因此集成测试配置使用 Netty unpooled 分配器并保留 `-Xcheck:jni`；只改变测试网络缓冲区，生产启动不改变池化设置。
普通池化模式另行通过实际服务测试。子图 JNI 已补充回调异常检查与传播回归。

打包统一使用 Maven Shade 生成 `target/Gremmunity-1.0-SNAPSHOT.jar` 并合并 SPI 服务描述。
原 assembly 的 `-all.jar` 会丢失 Groovy 脚本引擎注册，已停用；旧构建留下的 `-all.jar` 不要用于启动。
启动器将当前 JAR 按 SHA-256 复制到 `target/runtime/`，服务运行于该独立副本，
防止 Maven 重建主 JAR 导致运行中的 JVM 延迟加载类失败。不要手工覆盖正在使用的 runtime JAR。

2026-11-05 本机验证：Release JNI 和事务编译 JNI 分别通过 10/10 Maven 测试；
最终 Shade JAR 从资源加载 Bw JNI 后，HTTP Gremlin 写入/查询和正常关闭通过；Web 客户端构建通过。
Bw-Graph 的 Debug、Release、事务构建各 57/57 CTest 通过。测试日志保存在 Bw-Graph 仓库的
`output/monac-integration-20261105/`。Maven 依赖重叠和前端包体积提示仍保留，未宣称无警告构建。


### S3 / MinIO 存储模式

`bw-cloud-graph-java` 分支通过同一 Bw JNI 使用 S3 多尺度页存储；Gremlin API 不变。
使用 `conf/bw-cloud.properties` 和 `gremlin-server-bw-cloud.yaml`，不要直接复用本地模式的数据库目录。
凭据只从环境读取，不写入 properties、源码或浏览器。启动前设置：

```bash
export JAVA_HOME=/path/to/jdk
export S3_ENDPOINT=http://your-minio-host:9000
export S3_BUCKET=your-bucket
export S3_PREFIX=your-project/monac
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_REGION=us-east-1
bash scripts/bwcloud.sh start
# 另一个终端启动前端：
cd web-client
npm install
npm run dev -- --host 127.0.0.1 --strictPort
```

默认界面 `http://localhost:5173/`，Gremlin `ws://localhost:8182/gremlin`，
上传服务 `http://localhost:8284/`。云启动配置默认只监听本机。
`scripts/bwcloud.sh test` 运行真实 S3 的 Maven 集成测试，必须使用独立测试前缀。
如需事务编译 JNI，先在 Bw 仓库以 `--jni --txn` 构建，再将
`BWGRAPH_JNI_LIBRARY` 指向相应库后执行 test；Java 层仍不提供多语句事务。

六类页及其分配清单直接存 S3；异步读取仍进入已有 Buffer Pool frame。
ID/属性使用本地工作 RocksDB，正常关闭后上传远端 checkpoint；可在本地目录丢失后恢复。
这不提供崩溃恢复、属性逐次远端持久化或跨进程事务。远端 owner/dirty 标志不会自动抢占或清理。
保持 `S3_BUCKET`、`S3_PREFIX` 和图名一致；脚本设置的 `BWGRAPH_S3_LOCAL_ROOT`
使工作根目录下的相对图名可跨机器对应。Clear/upload 归档旧 generation，不删除历史页对象。

`CloudBackendTest` 额外覆盖本地工作目录移走后的恢复，以及空图重开不重复导入。
2026-11-06，恢复 MinIO 后，最终 Release 打包 JNI 和事务 JNI 各通过 11/11 Maven 集成测试；
Bw-Graph 的云端 Release/事务 CTest 各 58/58、12 组布局/索引/构建 SMO 和两组独立事务 MTR 均通过。
这些是分阶段验证，先前网络中断、运行脚本中断和运行 JAR 被覆盖的日志均保留；最终启动器改用
独立 runtime JAR，并已验证正常关闭、云端图重开和后端保持。证据位于 Bw-Graph 的
`output/s3-final-20261106/` 及相邻 `s3-recovered-*` 目录。
Web 前端构建和服务端真实上传测试已通过；浏览器最终交互验收仍被保存的站点权限阻止。
本次未验证 Linux 部署、云端性能或崩溃恢复。
