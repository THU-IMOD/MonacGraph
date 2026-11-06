/**
 * Gremlin Client Service
 * 连接到 Gremlin Server 并执行查询
 * 使用 HTTP REST API (浏览器兼容)
 */

import axios from 'axios'

class GremlinService {
  constructor() {
    this.baseUrl = null
    this.isConnected = false
    // Removed: this.sessionId = null  (now using sessionless mode)
  }

  /**
   * 连接到 Gremlin Server
   */
  async connect(host = 'localhost', port = 8182) {
    try {
      // 在开发环境使用代理，生产环境使用直接连接
      if (import.meta.env.DEV) {
        // 开发环境：使用 Vite 代理（避免 CORS）
        this.baseUrl = '/gremlin'
      } else {
        // 生产环境：直接连接
        this.baseUrl = `http://${host}:${port}/gremlin`
      }
      
      // Removed session ID generation - now using sessionless mode
      
      // 简单的连接测试（不操作数据库）
      const testQuery = '1+1'
      const response = await axios.post(this.baseUrl, {
        gremlin: testQuery
        // Removed: session parameter (sessionless mode)
      }, {
        headers: {
          'Content-Type': 'application/json'
        },
        timeout: 5000
      })

      if (response.status === 200) {
        this.isConnected = true
        console.log('✓ Connected to Gremlin Server (sessionless mode)')
        console.log('Using URL:', this.baseUrl)
        console.log('Mode: SESSIONLESS - All variables are global and persistent')
        console.log('Note: graph and g variables are shared globally')
        
        return { 
          success: true, 
          message: 'Connected successfully (sessionless mode). All variables are global.' 
        }
      }
    } catch (error) {
      this.isConnected = false
      console.error('✗ Failed to connect:', error)
      
      let errorMessage = error.message
      if (error.code === 'ECONNREFUSED') {
        errorMessage = `Cannot connect to Gremlin Server. Make sure it's running on port ${port}.`
      } else if (error.code === 'ERR_NETWORK' || error.response?.status === 0) {
        errorMessage = `Network error. This may be a CORS issue.`
      } else if (error.response) {
        const serverError = error.response.data?.message || error.response.statusText
        errorMessage = `Server error: ${error.response.status} - ${serverError}`
      }
      
      return { success: false, message: errorMessage }
    }
  }

  /**
   * 断开连接
   */
  async disconnect() {
    this.baseUrl = null
    this.isConnected = false
    // Removed: this.sessionId = null
    console.log('✓ Disconnected from Gremlin Server')
  }

  /**
   * 执行 Gremlin 查询
   */
  async executeQuery(query) {
    if (!this.isConnected) {
      throw new Error('Not connected to Gremlin Server')
    }

    try {
      console.log('Executing query (sessionless):', query)
      const startTime = Date.now()
      
      // 发送查询请求（sessionless 模式 - 全局上下文）
      const response = await axios.post(this.baseUrl, {
        gremlin: query,
        bindings: {},
        language: 'gremlin-groovy'
        // Removed: session parameter
        // All queries execute in global context
        // Variables are persistent and shared
      }, {
        headers: {
          'Content-Type': 'application/json'
        },
        timeout: 60000 // 60秒超时
      })
      
      const executionTime = Date.now() - startTime
      console.log(`✓ Query executed in ${executionTime}ms`)

      // 处理响应
      if (response.data && response.data.result) {
        const data = response.data.result.data
        
        // 检测是否是 Vset 结果
        const vsetResult = this.tryParseAsVsetResult(data)
        if (vsetResult) {
          console.log('✓ Detected Vset query result')
          return {
            success: true,
            data: data,
            vsetResult: vsetResult,
            isVset: true,
            executionTime,
            query
          }
        }
        
        return {
          success: true,
          data: data,
          executionTime,
          query
        }
      } else {
        return {
          success: true,
          data: [],
          executionTime,
          query
        }
      }
    } catch (error) {
      console.error('✗ Query error:', error)
      
      let errorMessage = error.message
      if (error.response && error.response.data) {
        errorMessage = error.response.data.message || JSON.stringify(error.response.data)
      }
      
      return {
        success: false,
        error: errorMessage,
        query
      }
    }
  }

  /**
   * 在当前 HTTP session 中初始化 g
   * 使用服务器启动时已经打开的 graph
   */
  async initializeSecondOrderInSession() {
    try {
      // 不要重新打开数据库，使用启动脚本打开的 graph
      await this.executeQuery(
        "g = graph.traversal(SecondOrderTraversalSource.class); " +
        "'g initialized in HTTP session'"
      )
      
      console.log('✓ g initialized in HTTP session using existing graph')
      return { success: true }
    } catch (error) {
      console.error('✗ Failed to initialize g in session:', error)
      return { success: false, error: error.message }
    }
  }

  /**
   * 初始化 SecondOrder Traversal Source
   * 这是为了支持 Gremmunity 的二阶逻辑查询
   * 注意：假设服务器端已经创建了 graph 对象
   */
  async initializeSecondOrderSession() {
    try {
      // 只初始化 g，不创建 graph（假设服务器端已经创建）
      await this.executeQuery(
        "g = graph.traversal(SecondOrderTraversalSource.class); " +
        "'SecondOrder session initialized'"
      )
      
      console.log('✓ SecondOrder session initialized (using existing graph)')
      return { success: true }
    } catch (error) {
      console.error('✗ Failed to initialize SecondOrder session:', error)
      // 不阻止连接，因为可能服务器不支持 SecondOrder
      // 但基本的 Gremlin 查询仍然可以工作
      console.log('Note: SecondOrder initialization failed, but basic queries should still work')
      return { success: false, error: error.message }
    }
  }

  /**
   * 获取整个图的数据（顶点和边）
   */
  async getGraphData() {
    try {
      // 先初始化 g
      const initQuery = "g = graph.traversal(SecondOrderTraversalSource.class); 'initialized'"
      const initResult = await this.executeQuery(initQuery)
      
      if (!initResult.success) {
        console.warn('Could not initialize g, trying direct query...')
      }
      
      // 获取所有顶点（使用 elementMap 避免序列化问题）
      const verticesQuery = 
        "g = graph.traversal(SecondOrderTraversalSource.class); " +
        "g.V().elementMap().toList()"
      const verticesResult = await this.executeQuery(verticesQuery)
      
      // 获取所有边
      const edgesQuery = 
        "g = graph.traversal(SecondOrderTraversalSource.class); " +
        "g.E().project('id', 'label', 'source', 'target')" +
        ".by(T.id)" +
        ".by(T.label)" +
        ".by(outV().id())" +
        ".by(inV().id())" +
        ".toList()"
      const edgesResult = await this.executeQuery(edgesQuery)

      if (!verticesResult.success || !edgesResult.success) {
        console.error('Failed to fetch graph data:', verticesResult, edgesResult)
        throw new Error('Failed to fetch graph data')
      }

      // 解开 GraphSON List 包装
      let vertices = this.unwrapGraphSONList(verticesResult.data) || []
      let edges = this.unwrapGraphSONList(edgesResult.data) || []
      
      console.log('✓ Fetched graph data:', vertices.length, 'vertices,', edges.length, 'edges')

      return {
        success: true,
        data: {
          nodes: this.formatVerticesFromElementMap(vertices),
          edges: this.formatEdgesFromProject(edges)
        }
      }
    } catch (error) {
      console.error('✗ Failed to get graph data:', error)
      return {
        success: false,
        error: error.message,
        data: { nodes: [], edges: [] }
      }
    }
  }

  /**
   * 解开 GraphSON List 包装
   */
  unwrapGraphSONList(data) {
    // 如果已经是数组，直接返回
    if (Array.isArray(data)) {
      return data
    }
    
    // 如果是 GraphSON List 格式：{@type: "g:List", @value: [...]}
    if (data && data['@type'] === 'g:List' && Array.isArray(data['@value'])) {
      console.log('✓ Unwrapped GraphSON List')
      return data['@value']
    }
    
    // 如果是单个 GraphSON 对象，包装成数组
    if (data && typeof data === 'object' && data['@type']) {
      console.log('✓ Wrapped single GraphSON object as array')
      return [data]
    }
    
    // 否则返回空数组
    console.warn('⚠ Could not unwrap data, returning empty array')
    return []
  }

  /**
   * 尝试将查询结果解析为图数据（公共方法，供store调用）
   */
  tryParseAsGraphData(data) {
    // 先解开 GraphSON List 包装
    const unwrapped = this.unwrapGraphSONList(data)
    
    if (!unwrapped || !Array.isArray(unwrapped) || unwrapped.length === 0) {
      return null
    }

    try {
      // 检查第一个元素的格式
      const first = unwrapped[0]
      
      // 检查是否是顶点数据（有id和label）
      let hasVertexStructure = false
      if (first && typeof first === 'object') {
        // GraphSON格式
        if (first['@type'] === 'g:Map') {
          const arr = first['@value'] || []
          const keys = []
          for (let i = 0; i < arr.length; i += 2) {
            const key = this.extractGraphSONValue(arr[i])
            keys.push(key)
          }
          hasVertexStructure = keys.includes('id') || keys.includes('label')
        }
        // 普通对象格式
        else {
          hasVertexStructure = 'id' in first || 'label' in first || 'T.id' in first || 'T.label' in first
        }
      }

      if (hasVertexStructure) {
        console.log('✓ Detected vertex data in query result')
        const nodes = this.formatVerticesFromElementMap(unwrapped)
        return { nodes, edges: [] }
      }

      // 检查是否是边数据
      if (first && (first.source !== undefined || first.target !== undefined)) {
        console.log('✓ Detected edge data in query result')
        const edges = this.formatEdgesFromProject(unwrapped)
        
        // 从边数据中提取节点ID
        const nodeIds = new Set()
        edges.forEach(edge => {
          if (edge.data.source) nodeIds.add(edge.data.source)
          if (edge.data.target) nodeIds.add(edge.data.target)
        })
        
        // 创建简单的节点数据（只有id和label）
        const nodes = Array.from(nodeIds).map(id => ({
          data: {
            id: String(id),
            label: 'node'  // 默认label
          }
        }))
        
        console.log(`✓ Created ${nodes.length} nodes from ${edges.length} edges`)
        return { nodes, edges }
      }

      return null
    } catch (error) {
      console.log('Could not parse as graph data:', error.message)
      return null
    }
  }

  /**
   * 尝试将查询结果解析为 Vset 结果
   * 支持两种 Vset 结果格式：
   * 
   * 格式 1（单个 Map）：
   * {
   *   "@type": "g:Map",
   *   "@value": [
   *     "type", "VsetResult",
   *     "subsets", [...],
   *     "totalCount", X
   *   ]
   * }
   * 
   * 格式 2（多个 Map）：
   * [
   *   {"@type": "g:Map", "@value": ["type", "VsetResult"]},
   *   {"@type": "g:Map", "@value": ["subsets", [...]]},
   *   {"@type": "g:Map", "@value": ["totalCount", X]}
   * ]
   */
  /**
   * 解析 GraphSON v3 格式
   */
  parseGraphSON(value) {
    // null/undefined 直接返回
    if (value === null || value === undefined) {
      return value
    }
    
    // 处理 GraphSON 类型包装 {@type: "g:Map", @value: [...]}
    if (value['@type'] && value['@value'] !== undefined) {
      const type = value['@type']
      const val = value['@value']
      
      switch (type) {
        case 'g:Map':
          // g:Map 格式: @value 是 [key1, val1, key2, val2, ...] 数组
          return this.parseGraphSONMap(val)
        
        case 'g:List':
          // g:List 格式: @value 是普通数组
          return val.map(item => this.parseGraphSON(item))
        
        case 'g:Set':
          // g:Set 格式: @value 是数组
          return val.map(item => this.parseGraphSON(item))
        
        case 'g:Int32':
        case 'g:Int64':
        case 'g:Double':
        case 'g:Float':
          // 数字类型直接返回值
          return val
        
        case 'g:UUID':
          // UUID 作为字符串
          return String(val)
        
        default:
          // 其他类型返回值
          return val
      }
    }
    
    // 普通对象，递归处理所有属性
    if (typeof value === 'object' && value !== null) {
      if (Array.isArray(value)) {
        return value.map(item => this.parseGraphSON(item))
      } else {
        const obj = {}
        for (const key in value) {
          obj[key] = this.parseGraphSON(value[key])
        }
        return obj
      }
    }
    
    // 基本类型直接返回
    return value
  }

  /**
   * 解析 GraphSON Map 格式
   */
  parseGraphSONMap(kvArray) {
    const obj = {}
    
    // 每次取两个元素：key 和 value
    for (let i = 0; i < kvArray.length; i += 2) {
      const key = kvArray[i]
      const value = kvArray[i + 1]
      obj[key] = this.parseGraphSON(value)
    }
    
    return obj
  }

  tryParseAsVsetResult(data) {
    try {
      console.log('🔍 Parsing Vset result, raw data:', data)
      
      // 使用新的 GraphSON 解析器
      const parsed = this.parseGraphSON(data)
      
      console.log('✓ GraphSON parsed:', parsed)
      
      // 检查是否是数组（查询可能返回数组）
      let obj = parsed
      if (Array.isArray(parsed)) {
        obj = {}
        for (const item of parsed) {
          Object.assign(obj, item)  // ← 合并所有 Map
        }
      }
      
      // 检查是否是 VsetResult
      if (!obj || obj.type !== 'VsetResult') {
        console.log('Not a Vset result (type:', obj?.type, ')')
        return null
      }

      console.log('✓ Detected Vset result format')
      
      // subsets 应该已经被正确解析了
      const subsets = obj.subsets || []
      
      console.log(`✓ Found ${subsets.length} subsets`)
      console.log('Subsets data:', subsets)

      return {
        type: 'VsetResult',
        subsets: subsets,
        totalCount: obj.totalCount || subsets.length
      }
    } catch (error) {
      console.error('❌ Error parsing Vset result:', error)
      console.error('Stack:', error.stack)
      return null
    }
  }

  /**
   * 格式化 elementMap 格式的顶点数据
   * 处理 GraphSON 格式：{@type: "g:Map", @value: [key1, val1, key2, val2, ...]}
   */
  formatVerticesFromElementMap(vertices) {
    return vertices.map((v, index) => {
      let id, label, properties = {}
      
      // 处理 GraphSON 格式
      if (v['@type'] === 'g:Map' && Array.isArray(v['@value'])) {
        const arr = v['@value']
        // 数组格式：[key1, value1, key2, value2, ...]
        for (let i = 0; i < arr.length; i += 2) {
          const key = this.extractGraphSONValue(arr[i])
          const value = this.extractGraphSONValue(arr[i + 1])
          
          if (key === 'id' || (typeof key === 'object' && key['@value'] === 'id')) {
            id = value
          } else if (key === 'label' || (typeof key === 'object' && key['@value'] === 'label')) {
            label = value
          } else {
            properties[key] = value
          }
        }
      } 
      // 处理普通 Map 格式
      else if (typeof v === 'object' && v !== null) {
        id = v['T.id'] || v.id || index
        label = v['T.label'] || v.label || 'vertex'
        
        Object.keys(v).forEach(key => {
          if (key !== 'T.id' && key !== 'T.label' && key !== 'id' && key !== 'label' && key !== '@type' && key !== '@value') {
            properties[key] = v[key]
          }
        })
      }

      // 确保有 id 和 label
      if (!id) id = index
      if (!label) label = 'vertex'

      return {
        data: {
          id: String(id),
          label: String(label),
          ...properties
        }
      }
    })
  }

  /**
   * 从 GraphSON 格式中提取实际值
   */
  extractGraphSONValue(obj) {
    if (obj === null || obj === undefined) {
      return obj
    }
    
    // 如果是 GraphSON 对象：{@type: "...", @value: ...}
    if (typeof obj === 'object' && '@value' in obj) {
      const type = obj['@type']
      const value = obj['@value']
      
      // 特殊处理 g:T 类型（T.id, T.label）
      if (type === 'g:T') {
        return value  // 返回 'id' 或 'label'
      }
      
      // 递归处理 g:List
      if (type === 'g:List' && Array.isArray(value)) {
        return value.map(item => this.extractGraphSONValue(item))
      }
      
      // 递归处理 g:Map
      if (type === 'g:Map' && Array.isArray(value)) {
        const result = {}
        for (let i = 0; i < value.length; i += 2) {
          const key = this.extractGraphSONValue(value[i])
          const val = this.extractGraphSONValue(value[i + 1])
          result[key] = val
        }
        return result
      }
      
      // 其他类型（g:Int32, g:Int64, etc.）直接返回值
      return value
    }
    
    // 普通值
    return obj
  }

  /**
   * 格式化 project 格式的边数据
   * 处理 GraphSON 格式
   */
  formatEdgesFromProject(edges) {
    return edges.map((e, index) => {
      let id, source, target, label
      
      // 处理 GraphSON 格式
      if (e['@type'] === 'g:Map' && Array.isArray(e['@value'])) {
        const arr = e['@value']
        for (let i = 0; i < arr.length; i += 2) {
          const key = this.extractGraphSONValue(arr[i])
          const value = this.extractGraphSONValue(arr[i + 1])
          
          if (key === 'id') {
            id = value
          } else if (key === 'source') {
            source = value
          } else if (key === 'target') {
            target = value
          } else if (key === 'label') {
            label = value
          }
        }
      } 
      // 处理普通对象格式
      else if (typeof e === 'object' && e !== null) {
        id = e.id
        source = e.source
        target = e.target
        label = e.label
      }

      return {
        data: {
          id: 'edge-' + String(id !== undefined ? id : index),  // 🔥 添加 'edge-' 前缀
          source: String(source),
          target: String(target),
          label: String(label || 'edge')
        }
      }
    })
  }

  /**
   * 格式化顶点数据为 Cytoscape 格式
   */
  formatVertices(vertices) {
    return vertices.map((v, index) => {
      // 处理不同的数据格式
      let id, label, properties = {}
      
      if (typeof v === 'object' && v !== null) {
        // 如果是对象
        id = v.id || v.T?.id || index
        label = v.label || v.T?.label || 'vertex'
        
        // 提取属性
        Object.keys(v).forEach(key => {
          if (key !== 'id' && key !== 'label' && key !== 'T') {
            const value = v[key]
            // 如果属性值是数组，取第一个元素
            properties[key] = Array.isArray(value) ? value[0] : value
          }
        })
      } else {
        // 如果是原始值
        id = index
        label = String(v)
      }

      return {
        data: {
          id: String(id),
          label: properties.name || label || String(id),
          type: label,
          ...properties
        }
      }
    })
  }

  /**
   * 格式化边数据为 Cytoscape 格式
   */
  formatEdges(edges) {
    return edges.map((e, index) => {
      let id, label, source, target, properties = {}
      
      if (typeof e === 'object' && e !== null) {
        id = e.id !== undefined ? e.id : index
        label = e.label || 'edge'
        source = String(e.source || e.outV || '')
        target = String(e.target || e.inV || '')
        
        // 提取属性
        if (e.properties) {
          Object.assign(properties, e.properties)
        }
      } else {
        id = index
        label = 'edge'
        source = ''
        target = ''
      }

      return {
        data: {
          id: 'edge-' + String(id),  // 🔥 添加 'edge-' 前缀
          source,
          target,
          label,
          ...properties
        }
      }
    })
  }

  /**
   * 创建测试数据（与Java客户端SecondOrderClient.java一致）
   */
  async createTestData() {
    try {
      // 执行完整的测试数据创建和查询
      const result = await this.executeQuery(`g = graph.traversal(SecondOrderTraversalSource.class);
alice = g.addV('person').property(T.id, 1).property('name', 'Alice').next();
bob = g.addV('person').property(T.id, 2).property('name', 'Bob').next();
charlie = g.addV('person').property(T.id, 3).property('name', 'Charlie').next();
david = g.addV('person').property(T.id, 4).property('name', 'David').next();
alice.addEdge('knows', bob);
bob.addEdge('knows', charlie);
charlie.addEdge('knows', alice);
result = g.Vset().forall('x').forall('y').filter('g.V(x).out("knows").is(y) || g.V(y).out("knows").is(x) || g.V(x).is(y)').execute(); 
result.size()`)

      if (!result.success) return result

      console.log('✓ Test data created')
      console.log('  - 4 vertices: Alice, Bob, Charlie, David')
      console.log('  - 3 edges: Alice->Bob, Bob->Charlie, Charlie->Alice')
      console.log('  - Note: David is isolated (no edges)')
      console.log('  - Vset query executed, result:', result)
      return { success: true, result }
    } catch (error) {
      console.error('✗ Failed to create test data:', error)
      return { success: false, error: error.message }
    }
  }

  /**
   * 清除所有数据
   */
  async clearGraph() {
    try {
      const result = await this.executeQuery("graph.clear(); 'Graph cleared'")
      if (!result.success) return result
      console.log('✓ Graph cleared')
      return { success: true }
    } catch (error) {
      console.error('✗ Failed to clear graph:', error)
      return { success: false, error: error.message }
    }
  }
}

// 导出单例
export default new GremlinService()
