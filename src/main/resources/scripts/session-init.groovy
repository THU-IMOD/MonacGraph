import com.graph.rocks.community.CommunityGraph
import com.graph.rocks.so.SecondOrderTraversalSource

graph = CommunityGraph.open('db')

g = graph.traversal(SecondOrderTraversalSource.class)