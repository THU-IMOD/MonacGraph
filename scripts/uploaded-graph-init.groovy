// Auto-generated initialization script for uploaded graph
// Generated at: 2026-03-17T17:26:23.077904100

// Initialize graph
graph.reload('example')
g = graph.traversal(SecondOrderTraversalSource.class)

// Load vertex properties
graph.loadVertexProperty('exampleVertexProperty.csv')

// Load edge properties
graph.loadEdgeProperty('exampleEdgeProperty.json')

println "Uploaded graph 'example' initialized"
