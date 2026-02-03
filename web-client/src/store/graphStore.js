/**
 * Graph Store - Pinia State Management
 * 管理图数据、查询历史、连接状态等
 */

import { defineStore } from 'pinia'
import gremlinClient from '@/services/gremlinClient'

export const useGraphStore = defineStore('graph', {
  state: () => ({
    // 连接状态
    isConnected: false,
    serverInfo: {
      host: 'localhost',
      port: 8182
    },

    // 图信息
    graphName: 'my_database', // 当前图名称

    // 图数据
    graphData: {
      nodes: [],
      edges: []
    },

    // 查询相关
    currentQuery: '',
    queryResult: null,
    queryHistory: [],
    isExecuting: false,

    // 选中的元素
    selectedElements: [],

    // UI 状态
    showStats: true,
    autoRefresh: false
  }),

  getters: {
    // 图统计信息
    graphStats: (state) => ({
      nodeCount: state.graphData.nodes.length,
      edgeCount: state.graphData.edges.length,
      nodeTypes: [...new Set(state.graphData.nodes.map(n => n.data.label))].filter(Boolean),
      edgeTypes: [...new Set(state.graphData.edges.map(e => e.data.label))].filter(Boolean)
    }),

    // 最近的查询
    recentQueries: (state) => {
      return state.queryHistory.slice(-10).reverse()
    }
  },

  actions: {
    /**
     * 连接到 Gremlin Server
     */
    async connect(host, port) {
      this.serverInfo = { host, port }
      const result = await gremlinClient.connect(host, port)
      
      if (result.success) {
        this.isConnected = true
        // 初始化 SecondOrder session
        await gremlinClient.initializeSecondOrderSession()
      }
      
      return result
    },

    /**
     * 断开连接
     */
    async disconnect() {
      await gremlinClient.disconnect()
      this.isConnected = false
      this.graphData = { nodes: [], edges: [] }
      this.queryResult = null
    },

    /**
     * 执行查询
     */
    async executeQuery(query) {
      if (!query || !query.trim()) {
        return { success: false, error: 'Empty query' }
      }

      this.isExecuting = true
      
      // 自动添加 g 初始化语句（如果查询中没有）
      let processedQuery = query.trim()
      const initStatement = 'g = graph.traversal(SecondOrderTraversalSource.class);'
      
      // 检查查询是否已包含g的初始化
      // 改进检测：检查是否有 g = 或 graph.traversal
      const hasInit = processedQuery.includes('graph.traversal') || 
                     processedQuery.match(/^\s*g\s*=/) ||
                     processedQuery.includes('g = ')
      
      if (!hasInit) {
        // 如果查询不包含初始化，自动添加
        // 注意：假设服务器端已经有graph对象
        processedQuery = initStatement + '\n' + processedQuery
        console.log('✓ Auto-prepended g initialization statement')
      }
      
      this.currentQuery = processedQuery

      try {
        const result = await gremlinClient.executeQuery(processedQuery)
        
        // 保存到历史（保存原始查询）
        this.queryHistory.push({
          query: query, // 保存用户输入的原始查询
          timestamp: new Date().toISOString(),
          success: result.success,
          executionTime: result.executionTime
        })

        if (result.success) {
          this.queryResult = result
          
          // 尝试将查询结果解析为图数据
          const graphData = this.tryParseGraphData(result.data)
          
          if (graphData && (graphData.nodes.length > 0 || graphData.edges.length > 0)) {
            // 如果查询返回了图数据，直接使用
            console.log('✓ Query returned graph data, updating visualization')
            this.graphData = graphData
          } else {
            // 否则刷新完整的图数据
            console.log('Query did not return graph data, refreshing full graph')
            await this.refreshGraphData()
          }
        }

        return result
      } finally {
        this.isExecuting = false
      }
    },

    /**
     * 尝试将查询结果解析为图数据
     */
    tryParseGraphData(data) {
      return gremlinClient.tryParseAsGraphData(data)
    },

    /**
     * 刷新图数据
     */
    async refreshGraphData() {
      const result = await gremlinClient.getGraphData()
      
      if (result.success) {
        this.graphData = result.data
      }
      
      return result
    },

    /**
     * 创建测试数据
     */
    async createTestData() {
      const result = await gremlinClient.createTestData()
      
      if (result.success) {
        await this.refreshGraphData()
      }
      
      return result
    },

    /**
     * 清除图数据
     */
    async clearGraph() {
      const result = await gremlinClient.clearGraph()
      
      if (result.success) {
        this.graphData = { nodes: [], edges: [] }
        this.queryResult = null
      }
      
      return result
    },

    /**
     * 设置选中的元素
     */
    setSelectedElements(elements) {
      this.selectedElements = elements
    },

    /**
     * 清除查询结果
     */
    clearQueryResult() {
      this.queryResult = null
    }
  }
})
