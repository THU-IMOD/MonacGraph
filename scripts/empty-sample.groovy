// ===== Gremmunity 初始化脚本 =====

// 直接创建全局变量 g
// graph 变量由 GremlinServerGremlinPlugin 自动注入
g = graph.traversal()

println "=" * 60
println "Gremmunity Initialized"
println "Graph type: " + graph.getClass().getName()
println "Traversal source 'g' is now available"
println "=" * 60

// 可选：示例数据加载函数
def loadSampleData() {
    println "Loading sample data..."

    g.addV('person').property('name', 'marko').property('age', 29).next()
    g.addV('person').property('name', 'vadas').property('age', 27).next()
    g.addV('person').property('name', 'josh').property('age', 32).next()
    g.addV('software').property('name', 'lop').property('lang', 'java').next()

    def marko = g.V().has('name', 'marko').next()
    def josh = g.V().has('name', 'josh').next()
    def lop = g.V().has('name', 'lop').next()

    g.addE('knows').from(marko).to(josh).property('weight', 1.0).next()
    g.addE('created').from(marko).to(lop).property('weight', 0.4).next()
    g.addE('created').from(josh).to(lop).property('weight', 0.4).next()

    def count = g.V().count().next()
    println "Sample data loaded: ${count} vertices"
    return count
}

// 可选：自动加载示例数据
// loadSampleData()