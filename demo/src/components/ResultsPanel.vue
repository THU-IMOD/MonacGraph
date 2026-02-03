<template>
  <div class="results-panel">
    <div class="panel-header">
      <h3>
        <el-icon><DocumentCopy /></el-icon>
        Query Results
      </h3>
      <el-button 
        type="text" 
        @click="closePanel"
        size="small"
      >
        <el-icon><Close /></el-icon>
      </el-button>
    </div>

    <div class="panel-content" v-if="store.queryResult">
      <!-- 查询信息 -->
      <el-alert
        :type="store.queryResult.success ? 'success' : 'error'"
        :closable="false"
        class="result-alert"
      >
        <template #title>
          <div class="alert-content">
            <span v-if="store.queryResult.success">
              ✓ Query executed successfully
            </span>
            <span v-else>
              ✗ Query failed
            </span>
            <el-tag 
              v-if="store.queryResult.executionTime" 
              size="small"
              type="info"
            >
              {{ store.queryResult.executionTime }}ms
            </el-tag>
          </div>
        </template>
      </el-alert>

      <!-- 错误信息 -->
      <el-alert
        v-if="!store.queryResult.success && store.queryResult.error"
        type="error"
        :title="store.queryResult.error"
        :closable="false"
        class="error-alert"
      />

      <!-- 结果数据 -->
      <div v-if="store.queryResult.success && store.queryResult.data" class="result-container">
        <el-tabs v-model="activeTab" class="result-tabs">
          <!-- 表格视图 -->
          <el-tab-pane label="Table" name="table">
            <div class="result-table">
              <div class="result-summary">
                <el-text type="info" size="small">
                  {{ resultCount }} result(s)
                </el-text>
              </div>
              
              <div class="native-scrollbar">
                <div v-if="isTableData" class="table-container">
                  <el-table 
                    :data="tableData" 
                    size="small"
                    stripe
                    border
                  >
                    <el-table-column
                      v-for="col in tableColumns"
                      :key="col"
                      :prop="col"
                      :label="col"
                      min-width="120"
                    >
                      <template #default="{ row }">
                        <span class="cell-value">{{ formatValue(row[col]) }}</span>
                      </template>
                    </el-table-column>
                  </el-table>
                </div>
                <div v-else class="list-container">
                  <el-tag
                    v-for="(item, index) in store.queryResult.data"
                    :key="index"
                    class="result-tag"
                    type="info"
                  >
                    {{ formatValue(item) }}
                  </el-tag>
                </div>
              </div>
            </div>
          </el-tab-pane>

          <!-- JSON 视图 -->
          <el-tab-pane label="JSON" name="json">
            <div class="json-view">
              <div class="native-scrollbar json-scroll-area">
                <el-button 
                  size="small" 
                  @click="copyToClipboard"
                  class="copy-button-in-code"
                >
                  <el-icon><CopyDocument /></el-icon>
                  Copy
                </el-button>
                <pre class="json-content">{{ formattedJSON }}</pre>
              </div>
            </div>
          </el-tab-pane>

          <!-- 统计视图 -->
          <el-tab-pane label="Stats" name="stats">
            <!-- 1. 给Stats的滚动容器添加专属类名 stats-scroll-container -->
            <div class="native-scrollbar stats-scroll-container">
              <div class="stats-view">
                <el-descriptions :column="1" border size="small">
                  <el-descriptions-item label="Result Count">
                    {{ resultCount }}
                  </el-descriptions-item>
                  <el-descriptions-item label="Result Type">
                    {{ resultType }}
                  </el-descriptions-item>
                  <el-descriptions-item label="Execution Time">
                    {{ store.queryResult.executionTime }}ms
                  </el-descriptions-item>
                  <el-descriptions-item label="Query">
                    <pre class="query-text">{{ store.queryResult.query }}</pre>
                  </el-descriptions-item>
                </el-descriptions>
              </div>
            </div>
          </el-tab-pane>
        </el-tabs>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { useGraphStore } from '@/store/graphStore'
import { ElMessage } from 'element-plus'
import { DocumentCopy, Close, CopyDocument } from '@element-plus/icons-vue'

const store = useGraphStore()
const activeTab = ref('json')

// 结果数量
const resultCount = computed(() => {
  if (!store.queryResult?.data) return 0
  return Array.isArray(store.queryResult.data) 
    ? store.queryResult.data.length 
    : 1
})

// 结果类型
const resultType = computed(() => {
  if (!store.queryResult?.data) return 'Unknown'
  const data = store.queryResult.data
  
  if (Array.isArray(data)) {
    if (data.length === 0) return 'Empty Array'
    if (typeof data[0] === 'object') return 'Array of Objects'
    return 'Array of Primitives'
  }
  
  return typeof data
})

// 判断是否为表格数据
const isTableData = computed(() => {
  const data = store.queryResult?.data
  if (!Array.isArray(data) || data.length === 0) return false
  return typeof data[0] === 'object' && data[0] !== null
})

// 表格列
const tableColumns = computed(() => {
  if (!isTableData.value) return []
  const firstItem = store.queryResult.data[0]
  return firstItem instanceof Map ? Array.from(firstItem.keys()) : Object.keys(firstItem)
})

// 表格数据
const tableData = computed(() => {
  if (!isTableData.value) return []
  return store.queryResult.data.map((item, index) => {
    if (item instanceof Map) {
      const obj = { _index: index }
      for (const [key, value] of item.entries()) obj[key] = value
      return obj
    }
    return { _index: index, ...item }
  })
})

// 格式化 JSON
const formattedJSON = computed(() => {
  if (!store.queryResult?.data) return ''
  try {
    const data = store.queryResult.data
    if (Array.isArray(data)) {
      const converted = data.map(item => item instanceof Map ? Object.fromEntries(item) : item)
      return JSON.stringify(converted, null, 2)
    }
    return JSON.stringify(data, null, 2)
  } catch (error) {
    return String(store.queryResult.data)
  }
})

// 格式化值
const formatValue = (value) => {
  if (value === null || value === undefined) return 'null'
  if (Array.isArray(value)) return value.join(', ')
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

// 复制到剪贴板
const copyToClipboard = () => {
  navigator.clipboard.writeText(formattedJSON.value)
    .then(() => ElMessage.success('Copied to clipboard'))
    .catch(() => ElMessage.error('Failed to copy'))
}

// 关闭面板
const closePanel = () => {
  store.clearQueryResult()
}
</script>

<style scoped lang="scss">
/* 根容器：强制占满父容器高度 */
.results-panel {
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
  border-radius: 20px;
}

/* 面板头部 */
.panel-header {
  padding: 12px 15px;
  border-bottom: 1px solid rgba(0, 0, 0, 0.08);
  display: flex;
  justify-content: space-between;
  align-items: center;
  background: rgba(102, 126, 234, 0.05);
  backdrop-filter: blur(10px);

  h3 {
    margin: 0;
    font-size: 16px;
    font-weight: 700;
    display: flex;
    align-items: center;
    gap: 8px;
    color: #2d3748;
  }
  
  .el-button {
    background: rgba(102, 126, 234, 0.1);
    backdrop-filter: blur(10px);
    border: 1px solid rgba(102, 126, 234, 0.3);
    color: #667eea;
    &:hover { background: rgba(102, 126, 234, 0.2); }
  }
}

/* 面板内容：强制传递高度 */
.panel-content {
  flex: 1;
  overflow: hidden;
  padding: 12px;
  display: flex;
  flex-direction: column;
}

/* 结果容器：承接面板内容的高度 */
.result-container {
  flex: 1;
  overflow: hidden;
  display: flex;
  flex-direction: column;
}

/* 标签页容器：修复Table标签被遮挡问题 */
.result-tabs {
  flex: 1;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  background: rgba(102, 126, 234, 0.05);
  backdrop-filter: blur(10px);
  border-radius: 12px;
  padding: 10px;
  border: 1px solid rgba(102, 126, 234, 0.1);

  :deep(.el-tabs__header) {
    background: rgba(102, 126, 234, 0.08);
    border-radius: 8px;
    padding: 5px 8px; /* 增加左右内边距，避免标签被挤压 */
    margin-bottom: 10px;
    border: none;
  }
  
  :deep(.el-tabs__nav-wrap) {
    width: 100%;
    display: flex !important;
    justify-content: center;
    padding: 0 5px; /* 给标签容器加左右内边距 */
    &::after { display: none !important; }
  }
  
  :deep(.el-tabs__nav) {
    border: none;
    float: none !important;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 4px; /* 缩小标签间距 */
    width: 100% !important; /* 让导航占满容器宽度 */
    flex: none;
  }
  
  :deep(.el-tabs__item) {
    color: #4a5568;
    font-weight: 600;
    border: none;
    // 1. 强制设置对称的左右内边距（关键：上下6px，左右12px）
    padding: 6px 12px !important; 
    // 2. 强制文字水平居中
    text-align: center !important;
    border-radius: 6px;
    white-space: nowrap;
    min-width: 60px;
    flex: 1;
    // 确保激活态/hover态也不改变内边距
    &.is-active {
      color: #667eea;
      background: rgba(102, 126, 234, 0.15);
      padding: 6px 12px !important; // 激活态也强制对称内边距
    }
    &:hover {
      color: #667eea;
      background: rgba(102, 126, 234, 0.08);
      padding: 6px 12px !important; // hover态也强制对称内边距
    }
  }
  
  :deep(.el-tabs__active-bar) { display: none; }
  
  :deep(.el-tabs__content) {
    flex: 1;
    overflow: hidden;
    display: flex;
    flex-direction: column;
  }
  
  :deep(.el-tab-pane) {
    height: 100%;
    overflow: hidden;
    display: flex;
    flex-direction: column;
  }
}

/* 原生滚动容器：确保滚动生效 */
.native-scrollbar {
  flex: 1;
  overflow: auto;
  min-height: 0;
  max-height: 100%;
}

/* 查询状态提示 */
.result-alert {
  margin-bottom: 10px;
  background: rgba(102, 126, 234, 0.1);
  backdrop-filter: blur(10px);
  border: 1px solid rgba(102, 126, 234, 0.2);
  border-radius: 12px;

  :deep(.el-alert__content) { color: #2d3748; }
  :deep(.el-alert__title) { color: #2d3748; font-weight: 600; }

  .alert-content {
    display: flex;
    justify-content: space-between;
    align-items: center;
    
    .el-tag {
      background: rgba(102, 126, 234, 0.15);
      border: 1px solid rgba(102, 126, 234, 0.3);
      color: #667eea;
      font-weight: 600;
    }
  }
}

/* 错误提示 */
.error-alert {
  margin-bottom: 10px;
  background: rgba(245, 87, 108, 0.1);
  backdrop-filter: blur(10px);
  border: 1px solid rgba(245, 87, 108, 0.3);
  border-radius: 12px;
  :deep(.el-alert__content) { color: #c53030; }
}

/* 表格视图 */
.result-table {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  
  .result-summary {
    margin-bottom: 10px;
    padding: 8px 10px;
    background: rgba(102, 126, 234, 0.08);
    backdrop-filter: blur(10px);
    border-radius: 8px;
    color: #2d3748;
    font-weight: 600;
    border: 1px solid rgba(102, 126, 234, 0.15);
  }

  .table-container {
    background: rgba(255, 255, 255, 0.5);
    border-radius: 8px;
    overflow: hidden;
    border: 1px solid rgba(0, 0, 0, 0.1);
    
    :deep(.el-table) {
      background: transparent;
      th {
        background: rgba(102, 126, 234, 0.08);
        color: #2d3748;
        font-weight: 700;
      }
      td {
        background: rgba(255, 255, 255, 0.5);
        border-bottom: 1px solid rgba(0, 0, 0, 0.05);
        color: #2d3748;
        &:hover { background: rgba(255, 255, 255, 0.7); }
      }
      tr:hover { background: rgba(102, 126, 234, 0.03); }
    }
    
    .cell-value {
      font-size: 13px;
      font-family: 'Fira Code', 'Courier New', monospace;
      color: #2d3748;
    }
  }

  .list-container {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    padding: 10px;

    .result-tag {
      font-family: 'Fira Code', 'Courier New', monospace;
      background: rgba(102, 126, 234, 0.15);
      backdrop-filter: blur(10px);
      border: 1px solid rgba(102, 126, 234, 0.3);
      color: #667eea;
      padding: 6px 12px;
      font-weight: 600;
    }
  }
}

/* JSON视图：修复滚动+按钮在代码框内 */
.json-view {
  flex: 1;
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
}

.json-scroll-area {
  flex: 1;
  height: 100%;
  min-height: 0;
  position: relative;
  padding: 5px; /* 给代码框加内边距 */
}

/* Copy按钮：在代码框内右上角 */
.copy-button-in-code {
  position: absolute;
  top: 15px;
  right: 15px;
  z-index: 10;
  background: rgba(102, 126, 234, 0.8);
  color: white;
  border: none;
  padding: 4px 8px;
  font-size: 12px;
  border-radius: 4px;
  &:hover {
    background: rgba(102, 126, 234, 1);
  }
}

.json-content {
  width: 100%;
  padding: 15px;
  background: rgba(0, 0, 0, 0.03);
  backdrop-filter: blur(10px);
  border-radius: 8px;
  font-size: 12px;
  font-family: 'Fira Code', 'Courier New', monospace;
  margin: 0;
  color: #2d3748;
  border: 1px solid rgba(0, 0, 0, 0.1);
  line-height: 1.6;
  white-space: pre;
  box-sizing: border-box;
  min-height: 100%; /* 确保代码框占满容器 */
}

.native-scrollbar {
  flex: 1;
  overflow: auto; /* 同时支持横向+纵向滚动 */
  min-height: 0;
  max-height: 100%;
}

/* Stats专属滚动容器：强制支持横竖滚动 */
.stats-scroll-container {
  /* 明确开启横向+纵向滚动 */
  overflow-x: auto;
  overflow-y: auto;
  /* 确保容器高度传递到位 */
  height: 100%;
  width: 100%;
}

/* 统计视图：设置最小宽度/高度，确保内容超出时触发滚动 */
.stats-view {
  padding: 10px;
  /* 设置最小宽度，确保横向滚动触发（可根据需要调整） */
  min-width: 600px;
  /* 设置最小高度，确保纵向滚动触发（可根据需要调整） */
  min-height: 400px;
  box-sizing: border-box;

  .el-descriptions {
    background: rgba(102, 126, 234, 0.05);
    backdrop-filter: blur(10px);
    border-radius: 12px;
    overflow: hidden;
    border: 1px solid rgba(102, 126, 234, 0.15);
    /* 让描述组件宽度跟随父容器，超出时触发横向滚动 */
    width: 100%;
    
    :deep(.el-descriptions__label) {
      color: #4a5568;
      font-weight: 700;
      background: rgba(102, 126, 234, 0.08);
    }
    
    :deep(.el-descriptions__content) {
      color: #2d3748;
      font-weight: 600;
      background: rgba(255, 255, 255, 0.5);
    }
  }

  .query-text {
    font-size: 12px;
    font-family: 'Fira Code', 'Courier New', monospace;
    margin: 10px 0 0 0;
    padding: 10px;
    background: rgba(0, 0, 0, 0.03);
    backdrop-filter: blur(10px);
    border-radius: 8px;
    max-height: 200px;
    /* 给查询文本也开启横向滚动 */
    overflow-x: auto;
    overflow-y: auto;
    color: #2d3748;
    border: 1px solid rgba(0, 0, 0, 0.1);
    line-height: 1.5;
    /* 确保长文本触发横向滚动 */
    white-space: pre;
    word-wrap: normal;
  }
}

</style>