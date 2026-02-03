<template>
  <div class="vset-browser">
    <!-- Header -->
    <div class="vset-header">
      <div class="vset-title">
        <el-icon><Collection /></el-icon>
        <h3>Vset Query Result</h3>
      </div>
      <div class="vset-info">
        <el-tag type="success">{{ totalSubsets }} subset(s) found</el-tag>
      </div>
    </div>

    <!-- Empty Result -->
    <div v-if="totalSubsets === 0" class="empty-result">
      <div class="empty-icon">Ø</div>
      <p>No subsets satisfy the condition</p>
    </div>

    <!-- Main Content -->
    <div v-else class="vset-main">
      <!-- Left: Graph and Navigation -->
      <div class="vset-left">
        <!-- Navigation -->
        <div class="subset-nav">
          <el-button
            :disabled="currentIndex === 0"
            @click="previousSubset"
            circle
          >
            <el-icon><ArrowLeft /></el-icon>
          </el-button>
          
          <div class="subset-indicator">
            <span class="current">{{ currentIndex + 1 }}</span>
            <span class="separator">/</span>
            <span class="total">{{ totalSubsets }}</span>
          </div>
          
          <el-button
            :disabled="currentIndex === totalSubsets - 1"
            @click="nextSubset"
            circle
          >
            <el-icon><ArrowRight /></el-icon>
          </el-button>
        </div>

        <!-- Current Subset Info -->
        <div class="current-subset-info">
          <el-descriptions :column="3" border size="small">
            <el-descriptions-item label="Index">
              {{ currentIndex + 1 }}
            </el-descriptions-item>
            <el-descriptions-item label="Size">
              {{ currentSubset.size }}
            </el-descriptions-item>
            <el-descriptions-item label="Vertices">
              {{ currentSubset.size === 0 ? 'Ø' : currentSubset.vertices.join(', ') }}
            </el-descriptions-item>
          </el-descriptions>
        </div>

        <!-- Graph Visualization -->
        <div class="subset-visualization">
          <!-- Toolbar -->
          <div class="subset-toolbar">
            <div class="toolbar-left">
              <span class="toolbar-title">Subset Graph</span>
            </div>
            <div class="toolbar-right">
              <el-button-group>
                <el-button 
                  size="small" 
                  @click="fitSubsetGraph"
                  :disabled="currentSubset.size === 0"
                >
                  <el-icon><FullScreen /></el-icon>
                  Fit
                </el-button>
                <el-button 
                  size="small" 
                  @click="resetSubsetGraph"
                  :disabled="currentSubset.size === 0"
                >
                  <el-icon><RefreshRight /></el-icon>
                  Reset
                </el-button>
                <el-button 
                  size="small" 
                  @click="cycleSubsetLayout"
                  :disabled="currentSubset.size === 0"
                >
                  <el-icon><Grid /></el-icon>
                  Layout: {{ subsetLayout }}
                </el-button>
              </el-button-group>
            </div>
          </div>

          <!-- Empty Set Display -->
          <div v-if="currentSubset.size === 0" class="empty-subset">
            <div class="empty-symbol">Ø</div>
            <p>Empty Set</p>
          </div>

          <!-- Graph -->
          <div v-else class="subset-graph" ref="subsetGraphContainer"></div>
        </div>

        <!-- Quick Jump -->
        <div class="quick-jump">
          <el-select
            v-model="currentIndex"
            placeholder="Jump to subset"
            size="small"
            style="width: 100%"
          >
            <el-option
              v-for="(subset, index) in subsets"
              :key="index"
              :label="`Subset ${index + 1} (${subset.size} vertices, ${(subset.edges || []).length} edges)`"
              :value="index"
            />
          </el-select>
        </div>
      </div>

      <!-- Right: Vertex List -->
      <div class="vset-right">
        <div class="vertex-list-panel">
          <h4>Subgraph ({{ currentSubset.size }} vertices, {{ (currentSubset.edges || []).length }} edges)</h4>
          
          <!-- Empty Set -->
          <div v-if="currentSubset.size === 0" class="no-vertices">
            <el-empty description="Empty Set (Ø)" :image-size="80" />
          </div>
          
          <!-- Vertex Cards -->
          <el-scrollbar v-else class="vertex-scrollbar">
            <el-card
              v-for="(props, vertexId) in currentSubset.properties"
              :key="vertexId"
              class="vertex-card"
              shadow="hover"
            >
              <div class="vertex-info">
                <div class="vertex-id">
                  <el-tag type="primary" size="small">ID: {{ vertexId }}</el-tag>
                  <el-tag size="small">{{ props.label }}</el-tag>
                </div>
                <div class="vertex-props">
                  <span
                    v-for="(value, key) in filterProperties(props)"
                    :key="key"
                    class="prop-item"
                  >
                    <strong>{{ key }}:</strong> {{ value }}
                  </span>
                </div>
              </div>
            </el-card>
          </el-scrollbar>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted, onUnmounted, nextTick } from 'vue'
import { Collection, ArrowLeft, ArrowRight, FullScreen, RefreshRight, Grid } from '@element-plus/icons-vue'
import cytoscape from 'cytoscape'

const props = defineProps({
  vsetResult: {
    type: Object,
    required: true
  }
})

// Vset data
const subsets = ref(props.vsetResult.subsets || [])
const totalSubsets = computed(() => subsets.value.length)
const currentIndex = ref(0)

// Adapt new format to old format for backward compatibility
const currentSubset = computed(() => {
  const subset = subsets.value[currentIndex.value] || { size: 0, vertices: [], edges: [] }
  
  // New format: vertices is array of objects like [{id: 1, label: "person", properties: {...}}]
  // Old format expected: properties is object like {1: {id: 1, label: "person", ...}}
  if (subset.size === 0) {
    return { size: 0, vertices: [], edges: [], properties: {} }
  }
  
  // Convert new format to compatible format
  const vertexIds = []
  const properties = {}
  
  for (const vertex of subset.vertices) {
    vertexIds.push(vertex.id)
    properties[vertex.id] = {
      id: vertex.id,
      label: vertex.label,
      ...vertex.properties
    }
  }
  
  return {
    size: subset.size,
    vertices: vertexIds,
    edges: subset.edges || [],
    properties: properties
  }
})

// Layout selection
const subsetLayout = ref('cose-bilkent')

// Cytoscape instance
const subsetGraphContainer = ref(null)
let cy = null
let isInitializing = false  // 防止重复初始化

// Navigation methods
const previousSubset = () => {
  if (currentIndex.value > 0) {
    currentIndex.value--
  }
}

const nextSubset = () => {
  if (currentIndex.value < totalSubsets.value - 1) {
    currentIndex.value++
  }
}

// Filter properties (remove id, label)
const filterProperties = (props) => {
  const filtered = { ...props }
  delete filtered.id
  delete filtered.label
  return filtered
}

// Initialize Cytoscape for current subset
const initSubsetGraph = async () => {
  // 防止重复初始化
  if (isInitializing) {
    console.log('⚠️ Already initializing, skipping...')
    return
  }
  
  // 检查子集是否为空
  if (currentSubset.value.size === 0) {
    console.log('⚠️ Empty subset, skipping graph init')
    return
  }
  
  console.log(`🔄 Starting graph init for subset ${currentIndex.value + 1} with ${currentSubset.value.size} nodes`)
  isInitializing = true

  try {
    // Wait for DOM update (multiple times for ref binding)
    await nextTick()
    await nextTick()
    await nextTick()
    
    // Wait for ref to be bound
    let refRetries = 0
    const maxRefRetries = 10
    
    while (!subsetGraphContainer.value && refRetries < maxRefRetries) {
      console.warn(`⏳ Waiting for container ref, retry ${refRetries + 1}/${maxRefRetries}`)
      await new Promise(resolve => setTimeout(resolve, 50))
      refRetries++
    }
    
    // Check if ref is available
    if (!subsetGraphContainer.value) {
      console.error('❌ No container ref after retries')
      isInitializing = false
      return
    }
    
    console.log('✅ Container ref found')
    
    // Wait for container to have size
    let sizeRetries = 0
    const maxSizeRetries = 10
    
    while (sizeRetries < maxSizeRetries) {
      const width = subsetGraphContainer.value.offsetWidth
      const height = subsetGraphContainer.value.offsetHeight
      
      if (width > 0 && height > 0) {
        console.log(`✅ Container ready: ${width}x${height}`)
        break
      }
      
      console.warn(`⏳ Container not ready (${width}x${height}), retry ${sizeRetries + 1}/${maxSizeRetries}`)
      await new Promise(resolve => setTimeout(resolve, 50))
      sizeRetries++
    }
    
    // Final check
    if (!subsetGraphContainer.value.offsetWidth || !subsetGraphContainer.value.offsetHeight) {
      console.error('❌ Container still has no size after retries')
      isInitializing = false
      return
    }

    // Destroy existing instance
    if (cy) {
      console.log('🗑️ Destroying old Cytoscape instance')
      cy.destroy()
      cy = null
    }

    // Create nodes from current subset
    const nodes = currentSubset.value.vertices.map(vertexId => {
      const props = currentSubset.value.properties[vertexId]
      return {
        data: {
          id: String(vertexId),
          label: props.label || 'node',
          ...props
        }
      }
    })

    // Create edges from current subset
    const edges = (currentSubset.value.edges || []).map(edge => {
      return {
        data: {
          id: "edge-" + String(edge.id),  // 添加 edge- 前缀避免ID冲突
          source: String(edge.source),
          target: String(edge.target),
          label: edge.label || 'edge',
          ...edge.properties
        }
      }
    })

    console.log(`📊 Creating Cytoscape with ${nodes.length} nodes and ${edges.length} edges`)

    // Combine nodes and edges
    const elements = [...nodes, ...edges]

    // Create Cytoscape instance
    cy = cytoscape({
      container: subsetGraphContainer.value,
      elements: elements,  // 包含节点和边
      style: [
        {
          selector: 'node',
          style: {
            'label': function(ele) {
              const data = ele.data()
              if (data.name) {
                return data.name
              }
              const keys = Object.keys(data)
              const systemProps = ['id', 'label', '@type', '@value']
              const userProps = keys.filter(k => !systemProps.includes(k))
              
              if (userProps.length > 0) {
                return data[userProps[0]] || data.id || data.label || ''
              }
              return data.id || data.label || ''
            },
            'text-valign': 'center',
            'text-halign': 'center',
            'background-color': '#667eea',
            'color': '#fff',
            'font-size': '14px',
            'font-weight': 'bold',
            'width': '80px',
            'height': '80px',
            'border-width': 3,
            'border-color': '#ffffff',
            'text-outline-width': 2,
            'text-outline-color': '#667eea'
          }
        },
        {
          selector: 'edge',
          style: {
            'width': 3,
            'line-color': '#9ca3af',
            'target-arrow-color': '#9ca3af',
            'target-arrow-shape': 'triangle',
            'curve-style': 'bezier',
            // 边标签 - 显示label和关键属性
            'label': function(ele) {
              const data = ele.data()
              let label = data.label || ''
              
              return label
            },
            'font-size': '12px',
            'color': '#4b5563',
            'text-rotation': 'autorotate',
            'text-margin-y': -10,
            'text-background-color': '#ffffff',
            'text-background-opacity': 0.8,
            'text-background-padding': '3px',
            'text-background-shape': 'roundrectangle'
          }
        },
        {
          selector: 'edge:selected',
          style: {
            'width': 4,
            'line-color': '#667eea',
            'target-arrow-color': '#667eea'
          }
        }
      ],
      layout: layouts[subsetLayout.value],
      minZoom: 0.5,
      maxZoom: 2,
      wheelSensitivity: 0.3
    })
    
    console.log(`✅ Cytoscape created, nodes: ${cy.nodes().length}, edges: ${cy.edges().length}`)
    
    // 创建 tooltip 元素
    let tooltipDiv = document.getElementById('vset-cy-tooltip')
    if (!tooltipDiv) {
      tooltipDiv = document.createElement('div')
      tooltipDiv.id = 'vset-cy-tooltip'
      tooltipDiv.style.cssText = `
        position: fixed;
        display: none;
        background: rgba(0, 0, 0, 0.9);
        color: white;
        padding: 10px 12px;
        border-radius: 8px;
        font-size: 12px;
        z-index: 10000;
        pointer-events: none;
        max-width: 300px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.3);
      `
      document.body.appendChild(tooltipDiv)
    }

    // 格式化属性为 HTML
    const formatProperties = (data) => {
      const lines = []
      for (const [key, value] of Object.entries(data)) {
        // 跳过内部属性
        if (key === 'source' || key === 'target') continue
        
        let displayValue = value
        if (typeof value === 'object') {
          displayValue = JSON.stringify(value)
        }
        
        lines.push(`<div style="margin: 2px 0;"><strong>${key}:</strong> ${displayValue}</div>`)
      }
      return lines.join('')
    }

    // 节点 hover 事件
    cy.on('mouseover', 'node', (evt) => {
      const node = evt.target
      const data = node.data()
      const pos = evt.renderedPosition
      
      tooltipDiv.innerHTML = `
        <div style="font-weight: bold; margin-bottom: 5px; border-bottom: 1px solid #fff; padding-bottom: 3px;">
          Node: ${data.name || data.id || 'Unknown'}
        </div>
        ${formatProperties(data)}
      `
      tooltipDiv.style.display = 'block'
      tooltipDiv.style.left = (pos.x + 15) + 'px'
      tooltipDiv.style.top = (pos.y + 15) + 'px'
    })

    // 边 hover 事件
    cy.on('mouseover', 'edge', (evt) => {
      const edge = evt.target
      const data = edge.data()
      const pos = evt.renderedPosition
      
      tooltipDiv.innerHTML = `
        <div style="font-weight: bold; margin-bottom: 5px; border-bottom: 1px solid #fff; padding-bottom: 3px;">
          Edge: ${data.label || 'Unknown'}
        </div>
        ${formatProperties(data)}
      `
      tooltipDiv.style.display = 'block'
      tooltipDiv.style.left = (pos.x + 15) + 'px'
      tooltipDiv.style.top = (pos.y + 15) + 'px'
    })

    // 鼠标移出时隐藏 tooltip
    cy.on('mouseout', 'node, edge', () => {
      tooltipDiv.style.display = 'none'
    })

    // 鼠标移动时更新 tooltip 位置
    cy.on('mousemove', 'node, edge', (evt) => {
      const pos = evt.renderedPosition
      tooltipDiv.style.left = (pos.x + 15) + 'px'
      tooltipDiv.style.top = (pos.y + 15) + 'px'
    })
    
    // Multiple attempts to center
    const centerGraph = () => {
      if (cy && cy.nodes().length > 0) {
        cy.fit(undefined, 50, undefined, false)  // 最后一个参数false禁用动画
        cy.center()
        const zoom = cy.zoom()
        const pan = cy.pan()
        console.log(`📍 Graph centered - zoom: ${zoom.toFixed(2)}, pan: (${pan.x.toFixed(0)}, ${pan.y.toFixed(0)})`)
      }
    }
    
    // Immediate center
    centerGraph()
    
    // Delayed center (insurance)
    setTimeout(centerGraph, 50)
    setTimeout(centerGraph, 150)
    setTimeout(centerGraph, 300)
    
    console.log('✅ Graph initialization complete')
  } catch (error) {
    console.error('❌ Error initializing graph:', error)
  } finally {
    isInitializing = false
  }
}

// Layout configurations
const layouts = {
  'cose-bilkent': {
    name: 'cose-bilkent',
    quality: 'default',
    nodeDimensionsIncludeLabels: true,
    refresh: 30,
    fit: true,
    padding: 60,
    randomize: true,
    idealEdgeLength: 100,
    edgeElasticity: 0.45,
    nestingFactor: 0.1,
    gravity: 0.6,
    numIter: 2500,
    tile: true,
    tilingPaddingVertical: 10,
    tilingPaddingHorizontal: 10,
    gravityRangeCompound: 1.5,
    gravityCompound: 1.0,
    gravityRange: 3.8,
    nodeRepulsion: 8000,
    animate: false  // 禁用动画
  },
  'circle': {
    name: 'circle',
    fit: true,
    padding: 50,
    avoidOverlap: true,
    nodeDimensionsIncludeLabels: true,
    animate: false  // 禁用动画
  },
  'grid': {
    name: 'grid',
    fit: true,
    padding: 50,
    avoidOverlap: true,
    nodeDimensionsIncludeLabels: true,
    rows: undefined,
    cols: undefined,
    animate: false  // 禁用动画
  },
  'concentric': {
    name: 'concentric',
    fit: true,
    padding: 50,
    avoidOverlap: true,
    nodeDimensionsIncludeLabels: true,
    concentric: function(node) {
      return node.degree()
    },
    levelWidth: function(nodes) {
      return 2
    },
    animate: false  // 禁用动画
  }
}

// Fit graph to view
const fitSubsetGraph = () => {
  if (cy) {
    cy.fit(undefined, 60, undefined, false)  // 禁用动画
    console.log('📐 Fitted subset graph to view')
  }
}

// Reset zoom and center
const resetSubsetGraph = () => {
  if (cy) {
    cy.fit(undefined, 60, undefined, false)  // 禁用动画
    cy.center()
    console.log('🔄 Reset subset graph view')
  }
}

// Change layout
const changeSubsetLayout = () => {
  if (cy && cy.nodes().length > 0) {
    console.log(`🎨 Changing subset layout to: ${subsetLayout.value}`)
    const layout = cy.layout(layouts[subsetLayout.value])
    
    layout.on('layoutstop', () => {
      cy.fit(undefined, 60, undefined, false)  // 禁用动画
      cy.center()
    })
    
    layout.run()
  }
}

// Cycle through layouts (for button click)
const cycleSubsetLayout = () => {
  const layoutOptions = ['cose-bilkent', 'circle', 'grid', 'concentric']
  const currentIdx = layoutOptions.indexOf(subsetLayout.value)
  const nextIdx = (currentIdx + 1) % layoutOptions.length
  subsetLayout.value = layoutOptions[nextIdx]
  changeSubsetLayout()
}

// Watch for subset changes
watch(currentIndex, (newIndex, oldIndex) => {
  console.log(`🔀 Subset changed: ${oldIndex} → ${newIndex}`)
  initSubsetGraph()
})

// 🔥 Watch for vsetResult prop changes (fix for consecutive queries)
watch(() => props.vsetResult, (newResult) => {
  console.log('🔄 VsetResult prop changed, updating subsets')
  subsets.value = newResult.subsets || []
  currentIndex.value = 0  // Reset to first subset
  
  // Destroy old graph if it exists
  if (cy) {
    cy.destroy()
    cy = null
  }
  
  // Re-initialize graph with new data
  if (subsets.value.length > 0 && subsets.value[0].size > 0) {
    nextTick(() => {
      initSubsetGraph()
    })
  }
}, { deep: true })

// Initialize on mount
onMounted(() => {
  console.log(`🚀 VsetBrowser mounted with ${totalSubsets.value} subsets`)
  if (totalSubsets.value > 0 && currentSubset.value.size > 0) {
    initSubsetGraph()
  }
})

// Cleanup on unmount
onUnmounted(() => {
  if (cy) {
    cy.destroy()
  }
  
  // 清理 tooltip 元素
  const tooltipDiv = document.getElementById('vset-cy-tooltip')
  if (tooltipDiv) {
    tooltipDiv.remove()
  }
})
</script>

<style scoped lang="scss">
.vset-browser {
  display: flex;
  flex-direction: column;
  gap: 12px;
  padding: 15px;
  background: transparent;  // 保持透明，因为它在graph-container内
  border-radius: 0;
  box-shadow: none;
  height: 100%;  // 使用100%而不是max-height
  overflow-y: auto;
}

.vset-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding-bottom: 12px;
  border-bottom: 2px solid rgba(0, 0, 0, 0.1);
  flex-shrink: 0;

  .vset-title {
    display: flex;
    align-items: center;
    gap: 12px;

    h3 {
      margin: 0;
      font-size: 18px;
      color: #2d3748;  // 改为深色
      font-weight: 700;
      text-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
    }
    
    .el-icon {
      font-size: 22px;
      color: #2d3748;  // 改为深色
    }
  }
  
  .vset-info {
    .el-tag {
      background: rgba(103, 194, 58, 0.15);
      backdrop-filter: blur(10px);
      border: 1px solid rgba(103, 194, 58, 0.4);
      color: #67c23a;
      font-weight: 700;
      font-size: 14px;
      padding: 6px 14px;
      border-radius: 20px;
    }
  }
}

.empty-result {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 60px 20px;
  color: #718096;

  .empty-icon {
    font-size: 80px;
    font-weight: bold;
    color: rgba(0, 0, 0, 0.2);
    margin-bottom: 20px;
    text-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
  }

  p {
    font-size: 16px;
    margin: 0;
    font-weight: 600;
    color: #4a5568;
  }
}

// Main two-column layout
.vset-main {
  display: flex;
  gap: 15px;
}

// Left column: Graph and navigation
.vset-left {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 12px;
  min-width: 0;
}

// Right column: Vertex list
.vset-right {
  width: 320px;
  display: flex;
  flex-direction: column;
  border-left: 2px solid rgba(0, 0, 0, 0.1);
  padding-left: 15px;
  flex-shrink: 0;
}

.subset-nav {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 20px;
  padding: 12px 15px;
  background: rgba(102, 126, 234, 0.08);
  backdrop-filter: blur(10px);
  border-radius: 12px;
  flex-shrink: 0;
  border: 1px solid rgba(102, 126, 234, 0.2);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);

  .el-button {
    background: rgba(102, 126, 234, 0.1);
    border: 1px solid rgba(102, 126, 234, 0.3);
    color: #667eea;
    width: 40px;
    height: 40px;
    border-radius: 50%;
    
    &:hover:not(:disabled) {
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      border-color: transparent;
      color: white;
      transform: scale(1.05);
      box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3);
    }
    
    &:disabled {
      opacity: 0.4;
    }
  }

  .subset-indicator {
    font-size: 18px;
    font-weight: 700;
    color: #2d3748;
    min-width: 90px;
    text-align: center;

    .current {
      color: #667eea;
      text-shadow: 0 0 8px rgba(102, 126, 234, 0.3);
      font-size: 22px;
    }

    .separator {
      margin: 0 8px;
      color: #a0aec0;
    }

    .total {
      color: #4a5568;
    }
  }
}

.current-subset-info {
  flex-shrink: 0;
  display: none;  /* 隐藏以节省空间，信息已在导航中显示 */
  
  .el-descriptions {
    background: rgba(102, 126, 234, 0.05);
    backdrop-filter: blur(10px);
    border-radius: 12px;
    overflow: hidden;
    border: 1px solid rgba(102, 126, 234, 0.15);
    
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
}

.subset-visualization {
  height: 450px;  /* 调整为450px */
  background: linear-gradient(135deg, 
    rgba(102, 126, 234, 0.03) 0%, 
    rgba(118, 75, 162, 0.03) 50%, 
    rgba(240, 147, 251, 0.03) 100%);
  border-radius: 12px;
  border: 1px solid rgba(0, 0, 0, 0.1);
  position: relative;
  flex-shrink: 0;
  overflow: hidden;
  backdrop-filter: blur(10px);
  box-shadow: inset 0 1px 4px rgba(0, 0, 0, 0.05);
  display: flex;
  flex-direction: column;
  
  /* Subtle grid pattern */
  background-image: 
    linear-gradient(rgba(102, 126, 234, 0.04) 1px, transparent 1px),
    linear-gradient(90deg, rgba(102, 126, 234, 0.04) 1px, transparent 1px);
  background-size: 30px 30px;

  .subset-toolbar {
    padding: 10px 12px;
    border-bottom: 1px solid rgba(0, 0, 0, 0.08);
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: rgba(102, 126, 234, 0.05);
    backdrop-filter: blur(10px);
    flex-shrink: 0;

    .toolbar-left {
      .toolbar-title {
        font-size: 14px;
        font-weight: 600;
        color: #2d3748;
      }
    }

    .toolbar-right {
      display: flex;
      align-items: center;
      gap: 8px;

      .el-button-group {
        background: rgba(102, 126, 234, 0.08);
        border-radius: 8px;
        overflow: hidden;
        border: 1px solid rgba(102, 126, 234, 0.2);

        .el-button {
          background: transparent;
          border: none;
          color: #667eea;
          font-weight: 600;
          padding: 6px 12px;
          font-size: 13px;

          &:hover:not(:disabled) {
            background: rgba(102, 126, 234, 0.15);
            color: #764ba2;
          }

          &:not(:last-child) {
            border-right: 1px solid rgba(102, 126, 234, 0.2);
          }

          &:disabled {
            opacity: 0.4;
            cursor: not-allowed;
          }
        }
      }

      .el-select {
        :deep(.el-input__wrapper) {
          background: rgba(102, 126, 234, 0.08);
          border: 1px solid rgba(102, 126, 234, 0.2);
          box-shadow: none;

          &:hover {
            background: rgba(102, 126, 234, 0.12);
          }

          &.is-focus {
            border-color: #667eea;
          }
        }
      }
    }
  }

  .empty-subset {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 100%;
    color: #718096;

    .empty-symbol {
      font-size: 80px;
      font-weight: bold;
      color: rgba(0, 0, 0, 0.15);
      margin-bottom: 15px;
      text-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }

    p {
      font-size: 16px;
      margin: 0;
      font-weight: 600;
      color: #4a5568;
    }
  }

  .subset-graph {
    width: 100%;
    flex: 1;
    height: 100%;
  }
}

.quick-jump {
  flex-shrink: 0;
  padding-top: 12px;
  border-top: 1px solid rgba(0, 0, 0, 0.1);
  
  :deep(.el-select) {
    width: 100%;
    
    .el-input__wrapper {
      background: rgba(102, 126, 234, 0.05);
      backdrop-filter: blur(10px);
      border: 1px solid rgba(102, 126, 234, 0.2);
      
      .el-input__inner {
        color: #2d3748;
      }
      
      &:hover {
        background: rgba(102, 126, 234, 0.08);
      }
    }
  }
}

// Vertex list panel (right side)
.vertex-list-panel {
  display: flex;
  flex-direction: column;
  height: 100%;
  
  h4 {
    margin: 0 0 12px 0;
    font-size: 15px;
    color: #2d3748;
    font-weight: 700;
    flex-shrink: 0;
    text-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
  }

  .no-vertices {
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    
    :deep(.el-empty) {
      .el-empty__description {
        color: #4a5568;
        font-weight: 600;
      }
    }
  }

  .vertex-scrollbar {
    flex: 1;
    min-height: 0;
  }

  .vertex-card {
    margin-bottom: 10px;
    background: rgba(102, 126, 234, 0.05);
    backdrop-filter: blur(10px);
    border: 1px solid rgba(102, 126, 234, 0.15);
    border-radius: 12px;
    transition: all 0.3s ease;

    &:hover {
      background: rgba(102, 126, 234, 0.1);
      transform: translateX(5px);
      box-shadow: 0 4px 12px rgba(102, 126, 234, 0.15);
    }

    :deep(.el-card__body) {
      padding: 12px;
    }

    .vertex-info {
      .vertex-id {
        display: flex;
        gap: 8px;
        margin-bottom: 8px;
        flex-wrap: wrap;
        
        .el-tag {
          background: rgba(102, 126, 234, 0.15);
          border: 1px solid rgba(102, 126, 234, 0.3);
          color: #667eea;
          font-weight: 700;
          
          &:nth-child(2) {
            background: rgba(118, 75, 162, 0.15);
            border-color: rgba(118, 75, 162, 0.3);
            color: #764ba2;
          }
        }
      }

      .vertex-props {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        font-size: 13px;
        color: #4a5568;
        line-height: 1.6;

        .prop-item {
          strong {
            color: #2d3748;
            font-weight: 700;
            text-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
          }
        }
      }
    }
  }
}
</style>
