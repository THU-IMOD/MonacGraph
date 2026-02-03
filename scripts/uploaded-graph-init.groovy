// Auto-generated initialization script for uploaded graph
// Generated at: 2026-02-03T19:53:34.052187700

// Initialize graph
graph.reload('misaka')
g = graph.traversal(SecondOrderTraversalSource.class)

// Load vertex properties
graph.loadVertexProperty('exampleVertexProperty.json')

// Load edge properties
graph.loadEdgeProperty('exampleEdgeProperty.json')

println "Uploaded graph 'misaka' initialized"
