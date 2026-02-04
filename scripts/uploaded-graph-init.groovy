// Auto-generated initialization script for uploaded graph
// Generated at: 2026-02-04T16:40:13.143282600

// Initialize graph
graph.reload('example')
g = graph.traversal(SecondOrderTraversalSource.class)

// Load vertex properties
graph.loadVertexProperty('exampleVertexProperty.json')

// Load edge properties
graph.loadEdgeProperty('exampleEdgeProperty.csv')

println "Uploaded graph 'example' initialized"
