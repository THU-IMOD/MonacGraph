// Auto-generated initialization script for uploaded graph
// Generated at: 2026-03-07T16:55:47.445198200

// Initialize graph
graph.reload('example')
g = graph.traversal(SecondOrderTraversalSource.class)

// Load vertex properties
graph.loadVertexProperty('exampleVertexProperty.csv')

// Load edge properties
graph.loadEdgeProperty('exampleEdgeProperty.json')

println "Uploaded graph 'example' initialized"
