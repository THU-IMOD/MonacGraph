<template>
  <div id="app" class="gremmunity-app">
    <!-- 顶部导航栏 -->
    <el-header class="app-header">
      <div class="header-left">
        <h1 class="app-title">
          MonacGraph
        </h1>
        <span class="subtitle">A Monadic Second-Order Logic Extended Graph
Database with Community-Aware Storage</span>
      </div>
      
      <div class="header-right">
        <!-- 连接状态 -->
        <el-tag :type="store.isConnected ? 'success' : 'danger'" size="large">
          <el-icon><Connection /></el-icon>
          {{ store.isConnected ? 'Connected' : 'Disconnected' }}
        </el-tag>

        <!-- 服务器信息 -->
        <span class="server-info" v-if="store.isConnected">
          {{ store.serverInfo.host }}:{{ store.serverInfo.port }}
        </span>

        <!-- 连接/断开按钮 -->
        <el-button 
          v-if="!store.isConnected"
          type="primary" 
          @click="showConnectionDialog = true"
        >
          <el-icon><Link /></el-icon>
          Connect
        </el-button>
        <el-button 
          v-else
          @click="handleDisconnect"
        >
          <el-icon><Close /></el-icon>
          Disconnect
        </el-button>
      </div>
    </el-header>

    <!-- 主内容区 -->
    <el-container class="app-container">
      <!-- 左侧边栏 -->
      <el-aside width="300px" class="sidebar">
        <el-tabs v-model="activeTab" class="sidebar-tabs">
          <!-- 数据标签页 -->
          <el-tab-pane label="Data" name="data">
            <div class="tab-content">
              <h3>Graph Statistics</h3>
              <el-descriptions :column="1" border size="small">
                <el-descriptions-item label="Graph Name">
                  <el-tag type="info">{{ store.graphName || 'my_database' }}</el-tag>
                </el-descriptions-item>
                <el-descriptions-item label="Nodes">
                  {{ store.graphStats.nodeCount }}
                </el-descriptions-item>
                <el-descriptions-item label="Edges">
                  {{ store.graphStats.edgeCount }}
                </el-descriptions-item>
                <el-descriptions-item label="Node Types">
                  <el-tag 
                    v-for="type in store.graphStats.nodeTypes" 
                    :key="type"
                    size="small"
                    style="margin: 2px;"
                  >
                    {{ type }}
                  </el-tag>
                </el-descriptions-item>
              </el-descriptions>

              <el-divider />

              <h3>Actions</h3>
              <div class="action-buttons">
                <el-button 
                  type="success"
                  @click="showUploadDialog = true"
                  :disabled="!store.isConnected"
                  style="width: 100%;"
                >
                  <el-icon><Upload /></el-icon>
                  Upload Graph
                </el-button>

                <el-button 
                  type="primary" 
                  @click="handleRefresh"
                  :loading="isRefreshing"
                  :disabled="!store.isConnected"
                  style="width: 100%;"
                >
                  <el-icon><Refresh /></el-icon>
                  Refresh Graph
                </el-button>

                <el-button 
                  @click="handleCreateTestData"
                  :disabled="!store.isConnected"
                  style="width: 100%;"
                >
                  <el-icon><Plus /></el-icon>
                  Create Test Data
                </el-button>

                <el-button 
                  type="danger" 
                  @click="handleClearGraph"
                  :disabled="!store.isConnected"
                  style="width: 100%;"
                >
                  <el-icon><Delete /></el-icon>
                  Clear Graph
                </el-button>
              </div>
            </div>
          </el-tab-pane>

          <!-- 查询历史标签页 -->
          <el-tab-pane label="History" name="history">
            <div class="tab-content">
              <h3>Query History</h3>
              <el-scrollbar height="600px">
                <div 
                  v-for="(item, index) in store.recentQueries" 
                  :key="index"
                  class="history-item"
                  @click="handleSelectHistoryQuery(item.query)"
                >
                  <div class="history-query">{{ item.query }}</div>
                  <div class="history-meta">
                    <el-tag :type="item.success ? 'success' : 'danger'" size="small">
                      {{ item.success ? '✓' : '✗' }}
                    </el-tag>
                    <span class="history-time">{{ item.executionTime }}ms</span>
                  </div>
                </div>
              </el-scrollbar>
            </div>
          </el-tab-pane>

          <!-- 示例查询标签页 -->
          <el-tab-pane label="Examples" name="examples">
            <div class="tab-content">
              <h3>Example Queries</h3>
              <el-collapse>
                <el-collapse-item 
                  v-for="(example, index) in exampleQueries" 
                  :key="index"
                  :title="example.title"
                >
                  <div class="example-description">{{ example.description }}</div>
                  <el-button 
                    size="small" 
                    type="primary"
                    @click="handleSelectQuery(example.query)"
                  >
                    Use This Query
                  </el-button>
                  <pre class="example-code">{{ example.query }}</pre>
                </el-collapse-item>
              </el-collapse>
            </div>
          </el-tab-pane>
        </el-tabs>
      </el-aside>

      <!-- 主内容区 -->
      <el-container class="main-content">
        <!-- 查询编辑器 -->
        <div class="query-section">
          <QueryEditor @execute="handleExecuteQuery" />
        </div>

        <!-- 图可视化和结果面板 -->
        <el-container class="visualization-section">
          <!-- 图可视化或Vset浏览器 -->
          <el-main class="graph-container">
            <!-- Vset结果浏览器 -->
            <VsetBrowser
              v-if="store.queryResult && store.queryResult.isVset"
              :vset-result="store.queryResult.vsetResult"
            />
            <!-- 普通图可视化 -->
            <GraphVisualization v-else />
          </el-main>

          <!-- 结果面板（Vset查询时隐藏） -->
          <el-aside 
            width="400px" 
            class="results-panel" 
            v-if="store.queryResult && !store.queryResult.isVset"
          >
            <ResultsPanel />
          </el-aside>
        </el-container>
      </el-container>
    </el-container>

    <!-- 连接对话框 -->
    <ConnectionDialog
      v-model="showConnectionDialog"
      :initial-host="connectionForm.host"
      :initial-port="connectionForm.port"
      :is-connecting="isConnecting"
      @connect="handleConnectionSubmit"
    />

    <!-- Graph Uploader Dialog -->
    <GraphUploader
      v-model:visible="showUploadDialog"
      @success="handleUploadSuccess"
    />
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useGraphStore } from '@/store/graphStore'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Connection, Link, Close, Refresh, Plus, Delete, Upload } from '@element-plus/icons-vue'
import QueryEditor from '@/components/QueryEditor.vue'
import GraphVisualization from '@/components/GraphVisualization.vue'
import ResultsPanel from '@/components/ResultsPanel.vue'
import VsetBrowser from '@/components/VsetBrowser.vue'
import GraphUploader from '@/components/GraphUploader.vue'
import ConnectionDialog from '@/components/ConnectionDialog.vue'

const store = useGraphStore()

// 状态
const activeTab = ref('data')
const showConnectionDialog = ref(false)
const showUploadDialog = ref(false)
const isConnecting = ref(false)
const isRefreshing = ref(false)

// 连接表单
const connectionForm = ref({
  host: 'localhost',
  port: 8182
})

// 示例查询
const exampleQueries = ref([
  {
    title: 'Create Test Data with Second-Order Query',
    description: 'Create 4 people and check connectivity',
    query: `g = graph.traversal(SecondOrderTraversalSource.class);
alice = g.addV('person').property(T.id, 1).property('name', 'Alice').next();
bob = g.addV('person').property(T.id, 2).property('name', 'Bob').next();
charlie = g.addV('person').property(T.id, 3).property('name', 'Charlie').next();
david = g.addV('person').property(T.id, 4).property('name', 'David').next();
alice.addEdge('knows', bob);
bob.addEdge('knows', charlie);
charlie.addEdge('knows', alice);
result = g.Vset().forall('x').forall('y').filter('g.V(x).out("knows").is(y) || g.V(y).out("knows").is(x) || g.V(x).is(y)').execute(); 
result.size()`
  },
  {
    title: 'Get All Vertices',
    description: 'Retrieve all vertices in the graph',
    query: 'g.V().elementMap().toList()'
  },
  {
    title: 'Count Vertices and Edges',
    description: 'Get basic graph statistics',
    query: `['vertices': g.V().count().next(), 'edges': g.E().count().next()]`
  },
  {
    title: 'Second-Order: Everyone Knows Someone',
    description: 'Check if every person knows at least one other person',
    query: `g.SecondOrder()
  .forall('x')
  .exist('y')
  .filter('g.V(x).outE().is(y)')
  .execute()`
  },
  {
    title: 'Vset: Find All Cliques with Size > 1',
    description: 'Find all cliques with size > 1 using second-order logic',
    query: `g.Vset()
  .forall('x')
  .forall('y')
  .filter('g.V(x).bothE().otherV().is(y) || g.V(x).is(y)')
  .having('size > 1')
  .executeForWeb()`
  },
  {
    title: 'WCC: Find Weakly Connected Components',
    description: 'Find all weakly connected components in the graph',
    query: 'g.WCC().executeForWeb()'
  },
  {
    title: 'SCC: Find Strongly Connected Components',
    description: 'Find all strongly connected components in the graph',
    query: 'g.SCC().executeForWeb()'
  },
  {
    title: 'Communities: Get LSM-Communities',
    description: 'Get all LSM-Communities in the graph',
    query: 'g.Community().executeForWeb()'
  },
  {
    title: 'BFS: Reachable Vertices',
    description: 'Find all vertices reachable from vertex 1',
    query: 'g.BFS(1).executeForWeb()'
  }
])

// 连接处理
const handleConnect = async () => {
  isConnecting.value = true
  try {
    const result = await store.connect(
      connectionForm.value.host,
      connectionForm.value.port
    )
    
    if (result.success) {
      ElMessage.success('Connected to Gremlin Server')
      showConnectionDialog.value = false
      // 加载图数据
      await store.refreshGraphData()
    } else {
      ElMessage.error(`Connection failed: ${result.message}`)
    }
  } finally {
    isConnecting.value = false
  }
}

// Handle connection from new dialog component
const handleConnectionSubmit = async (data) => {
  // Update connection form
  connectionForm.value.host = data.host
  connectionForm.value.port = data.port
  
  // Call original connect method
  await handleConnect()
}

const handleDisconnect = async () => {
  await store.disconnect()
  ElMessage.info('Disconnected from server')
}

// 查询处理
const handleExecuteQuery = async (query) => {
  const result = await store.executeQuery(query)
  
  if (result.success) {
    ElMessage.success(`Query executed in ${result.executionTime}ms`)
  } else {
    ElMessage.error(`Query failed: ${result.error}`)
  }
}

const handleSelectQuery = (query) => {
  store.currentQuery = query
}

const handleSelectHistoryQuery = (query) => {
  store.currentQuery = query
  activeTab.value = 'data'
}

// 数据操作
const handleRefresh = async () => {
  isRefreshing.value = true
  try {
    // 如果当前是Vset模式，清除查询结果
    if (store.queryResult && store.queryResult.isVset) {
      store.queryResult = null
      console.log('🔄 Cleared Vset result, switching to graph mode')
    }
    
    await store.refreshGraphData()
    ElMessage.success('Graph data refreshed')
  } finally {
    isRefreshing.value = false
  }
}

const handleCreateTestData = async () => {
  try {
    await ElMessageBox.confirm(
      'This will create test vertices (Alice, Bob, Charlie, David) and edges. Continue?',
      'Create Test Data',
      { type: 'info' }
    )
    
    await store.createTestData()
    ElMessage.success('Test data created')
  } catch {
    // User cancelled
  }
}

const handleClearGraph = async () => {
  try {
    await ElMessageBox.confirm(
      'This will delete all vertices and edges. This action cannot be undone!',
      'Clear Graph',
      { type: 'warning' }
    )
    
    await store.clearGraph()
    ElMessage.success('Graph cleared')
  } catch {
    // User cancelled
  }
}

const handleUploadSuccess = async (result) => {
  console.log('Graph uploaded successfully:', result)
  
  // Update the current graph name
  if (result.graphName) {
    store.graphName = result.graphName
    console.log('Updated graph name to:', result.graphName)
  }
  
  // Refresh graph data to show the newly loaded graph
  await handleRefresh()
  
  // Optionally, you can set the generated code to the query editor
  // (This would require adding a method to the store to update the current query)
}

// 初始化
onMounted(async () => {
  // 自动连接
  showConnectionDialog.value = true
})
</script>

<style scoped lang="scss">
.gremmunity-app {
  height: 100vh;
  display: flex;
  flex-direction: column;
  background: transparent;
  position: relative;
  overflow: hidden;
}

.app-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 30px;
  height: 80px;
  background: rgba(255, 255, 255, 0.1);
  backdrop-filter: blur(20px);
  -webkit-backdrop-filter: blur(20px);
  border-bottom: 1px solid rgba(255, 255, 255, 0.2);
  box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.2);
  position: relative;
  z-index: 100;

  /* Glass gradient overlay */
  &::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: linear-gradient(135deg, 
      rgba(102, 126, 234, 0.3) 0%, 
      rgba(118, 75, 162, 0.2) 50%, 
      rgba(240, 147, 251, 0.3) 100%);
    z-index: -1;
  }

  .header-left {
    display: flex;
    align-items: center;
    gap: 20px;
  }

  .app-title {
    margin: 0;
    font-size: 28px;
    font-weight: 700;
    display: flex;
    align-items: center;
    color: white;
    text-shadow: 0 2px 10px rgba(0, 0, 0, 0.2);
    letter-spacing: -0.5px;
  }

  .subtitle {
    font-size: 14px;
    opacity: 0.95;
    color: white;
    font-weight: 500;
    background: rgba(255, 255, 255, 0.15);
    padding: 6px 12px;
    border-radius: 20px;
    backdrop-filter: blur(5px);
  }

  .header-right {
    display: flex;
    align-items: center;
    gap: 15px;

    .el-tag {
      background: rgba(255, 255, 255, 0.2);
      backdrop-filter: blur(10px);
      border: 1px solid rgba(255, 255, 255, 0.3);
      color: white;
      font-weight: 600;
      padding: 8px 16px;
      font-size: 13px;
      
      &.el-tag--success {
        background: rgba(103, 194, 58, 0.3);
        border-color: rgba(103, 194, 58, 0.5);
      }
      
      &.el-tag--danger {
        background: rgba(245, 87, 108, 0.3);
        border-color: rgba(245, 87, 108, 0.5);
      }
    }
  }

  .server-info {
    font-size: 13px;
    opacity: 0.95;
    color: white;
    font-weight: 500;
    background: rgba(255, 255, 255, 0.15);
    padding: 6px 12px;
    border-radius: 20px;
  }

  .el-button {
    background: rgba(255, 255, 255, 0.2);
    backdrop-filter: blur(10px);
    border: 1px solid rgba(255, 255, 255, 0.3);
    color: white;
    font-weight: 600;
    
    &:hover {
      background: rgba(255, 255, 255, 0.3);
      transform: translateY(-2px);
      box-shadow: 0 5px 15px rgba(0, 0, 0, 0.2);
    }
    
    &.el-button--primary {
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      border: none;
      
      &:hover {
        background: linear-gradient(135deg, #764ba2 0%, #f093fb 100%);
        box-shadow: 0 5px 20px rgba(102, 126, 234, 0.4);
      }
    }
  }
}

.app-container {
  flex: 1;
  overflow: hidden;
  padding: 20px;
  gap: 20px;
  display: flex;
}

.sidebar {
  background: rgba(255, 255, 255, 0.92);
  backdrop-filter: blur(20px);
  -webkit-backdrop-filter: blur(20px);
  border: 1px solid rgba(0, 0, 0, 0.1);
  border-radius: 20px;
  box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.15);
  overflow: hidden;
  margin-right: 0;

  .sidebar-tabs {
    height: 100%;
    
    :deep(.el-tabs__header) {
      background: rgba(102, 126, 234, 0.08);
      backdrop-filter: blur(10px);
      margin: 0;
      padding: 10px 15px;
      border-bottom: 1px solid rgba(0, 0, 0, 0.1);
    }
    
    :deep(.el-tabs__nav-wrap) {
      padding: 0;
    }
    
    :deep(.el-tabs__nav) {
      display: flex;
      width: 100%;
      border: none;
    }
    
    :deep(.el-tabs__item) {
      flex: 1;
      text-align: center;
      padding: 0 10px;
      color: #4a5568;
      font-weight: 600;
      border: none;
      
      &.is-active {
        color: #667eea;
        background: rgba(102, 126, 234, 0.15);
        border-radius: 8px;
      }
      
      &:hover {
        color: #667eea;
        background: rgba(102, 126, 234, 0.08);
        border-radius: 8px;
      }
    }
    
    :deep(.el-tabs__active-bar) {
      display: none;
    }
    
    :deep(.el-tabs__content) {
      padding: 0;
      height: calc(100% - 60px);
      overflow-y: auto;
    }
  }

  .tab-content {
    padding: 20px;

    h3 {
      margin: 0 0 15px 0;
      font-size: 15px;
      font-weight: 700;
      color: #2d3748;
      text-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
    }
    
    .el-descriptions {
      background: rgba(255, 255, 255, 0.95);
      backdrop-filter: blur(10px);
      border-radius: 12px;
      overflow: hidden;
      border: 1px solid rgba(0, 0, 0, 0.1);
      
      :deep(.el-descriptions__label) {
        color: #4a5568;
        font-weight: 600;
        background: rgba(102, 126, 234, 0.08);
      }
      
      :deep(.el-descriptions__content) {
        color: #2d3748;
        background: rgba(255, 255, 255, 0.5);
      }
    }
  }

  .action-buttons {
    display: flex;
    flex-direction: column;
    gap: 10px;
    margin-top: 15px;
    
    .el-button {
      width: 100%;
      margin: 0;
      padding: 12px;
      justify-content: flex-start;
      background: rgba(255, 255, 255, 0.9);
      backdrop-filter: blur(10px);
      border: 1px solid rgba(0, 0, 0, 0.1);
      color: #2d3748;
      font-weight: 600;
      border-radius: 12px;
      
      .el-icon {
        margin-right: 8px;
      }
      
      &:hover:not(:disabled) {
        background: rgba(255, 255, 255, 1);
        transform: translateX(5px);
        box-shadow: 0 5px 15px rgba(0, 0, 0, 0.15);
      }
      
      &.el-button--primary {
        background: linear-gradient(135deg, rgba(102, 126, 234, 0.9) 0%, rgba(118, 75, 162, 0.9) 100%);
        border: 1px solid rgba(102, 126, 234, 0.5);
        color: white;
        
        &:hover {
          background: linear-gradient(135deg, rgba(118, 75, 162, 1) 0%, rgba(240, 147, 251, 1) 100%);
          box-shadow: 0 5px 20px rgba(102, 126, 234, 0.3);
        }
      }
      
      &.el-button--success {
        background: linear-gradient(135deg, rgba(103, 194, 58, 0.9) 0%, rgba(64, 158, 255, 0.9) 100%);
        border: 1px solid rgba(103, 194, 58, 0.5);
        color: white;
        
        &:hover {
          background: linear-gradient(135deg, rgba(103, 194, 58, 1) 0%, rgba(64, 158, 255, 1) 100%);
        }
      }
      
      &.el-button--danger {
        background: linear-gradient(135deg, rgba(245, 87, 108, 0.9) 0%, rgba(230, 75, 86, 0.9) 100%);
        border: 1px solid rgba(245, 87, 108, 0.5);
        color: white;
        
        &:hover {
          background: linear-gradient(135deg, rgba(245, 87, 108, 1) 0%, rgba(230, 75, 86, 1) 100%);
        }
      }
      
      &:disabled {
        opacity: 0.5;
        cursor: not-allowed;
      }
    }
  }

  .history-item {
    padding: 12px;
    margin-bottom: 10px;
    background: rgba(102, 126, 234, 0.08);
    backdrop-filter: blur(10px);
    border-radius: 12px;
    border: 1px solid rgba(102, 126, 234, 0.2);
    cursor: pointer;
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);

    &:hover {
      background: rgba(102, 126, 234, 0.15);
      transform: translateX(5px);
      box-shadow: 0 5px 15px rgba(102, 126, 234, 0.2);
      border-color: rgba(102, 126, 234, 0.4);
    }

    .history-query {
      font-size: 12px;
      font-family: 'Fira Code', 'Courier New', monospace;
      margin-bottom: 8px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      color: #2d3748;
      font-weight: 500;
    }

    .history-meta {
      display: flex;
      justify-content: space-between;
      align-items: center;
      font-size: 11px;
      
      .el-tag {
        background: rgba(102, 126, 234, 0.15);
        border: 1px solid rgba(102, 126, 234, 0.3);
        color: #667eea;
      }
      
      .history-time {
        color: #718096;
        font-weight: 600;
      }
    }
  }

  .example-description {
    margin-bottom: 8px;  /* 从12px减少到8px */
    font-size: 13px;
    color: #4a5568;
    line-height: 1.4;  /* 从1.5减少到1.4 */
  }

  .example-code {
    margin-top: 8px;  /* 从12px减少到8px */
    padding: 10px;  /* 从12px减少到10px */
    background: rgba(0, 0, 0, 0.05);
    backdrop-filter: blur(10px);
    border-radius: 8px;
    font-size: 11px;
    font-family: 'Fira Code', 'Courier New', monospace;
    overflow-x: auto;
    color: #2d3748;
    border: 1px solid rgba(0, 0, 0, 0.1);
    line-height: 1.5;  /* 从1.6减少到1.5 */
  }
  
  :deep(.el-collapse) {
    border: none;
    background: transparent;
    
    .el-collapse-item {
      margin-bottom: 6px;  /* 从10px减少到6px */
      background: rgba(102, 126, 234, 0.08);
      backdrop-filter: blur(10px);
      border-radius: 12px;
      border: 1px solid rgba(102, 126, 234, 0.2);
      overflow: hidden;
      
      .el-collapse-item__header {
        background: rgba(102, 126, 234, 0.05);
        color: #2d3748;
        font-weight: 600;
        padding: 10px 12px;  /* 从12px 15px减少到10px 12px */
        border: none;
        
        &:hover {
          background: rgba(102, 126, 234, 0.1);
        }
      }
      
      .el-collapse-item__wrap {
        background: transparent;
        border: none;
      }
      
      .el-collapse-item__content {
        padding: 12px;  /* 从15px减少到12px */
        color: #4a5568;
      }
    }
  }
}

.main-content {
  display: flex;
  flex-direction: column;
  overflow: hidden;
  flex: 1;
  gap: 15px;
}

.query-section {
  background: rgba(255, 255, 255, 0.92);
  backdrop-filter: blur(20px);
  -webkit-backdrop-filter: blur(20px);
  border: 1px solid rgba(0, 0, 0, 0.1);
  border-radius: 20px;
  padding: 12px 20px 10px 20px;
  box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.15);
}

.visualization-section {
  flex: 1;
  overflow: hidden;
  display: flex;
  gap: 20px;
}

.graph-container {
  flex: 1;
  background: rgba(255, 255, 255, 0.92);
  backdrop-filter: blur(20px);
  -webkit-backdrop-filter: blur(20px);
  border: 1px solid rgba(0, 0, 0, 0.1);
  border-radius: 20px;
  box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.15);
  padding: 0;
  overflow: hidden;
}

.results-panel {
  width: 400px;
  background: rgba(255, 255, 255, 0.92);
  backdrop-filter: blur(20px);
  -webkit-backdrop-filter: blur(20px);
  border: 1px solid rgba(0, 0, 0, 0.1);
  border-radius: 20px;
  box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.15);
  overflow: hidden;  /* 改为hidden，避免外层滚动条 */
  padding: 0;
}

/* Dialog Styles */
:deep(.el-dialog) {
  background: rgba(255, 255, 255, 0.95);
  backdrop-filter: blur(20px);
  border-radius: 20px;
  border: 1px solid rgba(255, 255, 255, 0.3);
  box-shadow: 0 25px 50px rgba(0, 0, 0, 0.3);
  
  .el-dialog__header {
    background: linear-gradient(135deg, rgba(102, 126, 234, 0.1) 0%, rgba(118, 75, 162, 0.1) 100%);
    border-radius: 20px 20px 0 0;
    border-bottom: 1px solid rgba(255, 255, 255, 0.3);
  }
  
  .el-dialog__title {
    font-weight: 700;
    color: #2d3748;
  }
  
  .el-dialog__body {
    padding: 30px;
  }
}

/* Form Styles */
:deep(.el-form-item__label) {
  color: #2d3748;
  font-weight: 600;
}

:deep(.el-input__wrapper) {
  background: rgba(255, 255, 255, 0.5);
  backdrop-filter: blur(10px);
  border: 1px solid rgba(102, 126, 234, 0.3);
  border-radius: 8px;
  box-shadow: none;
  
  &:hover, &.is-focus {
    background: rgba(255, 255, 255, 0.7);
    border-color: rgba(102, 126, 234, 0.5);
    box-shadow: 0 0 0 4px rgba(102, 126, 234, 0.1);
  }
}

/* Tag Styles */
:deep(.el-tag) {
  border-radius: 20px;
  font-weight: 600;
  padding: 4px 12px;
}
</style>
