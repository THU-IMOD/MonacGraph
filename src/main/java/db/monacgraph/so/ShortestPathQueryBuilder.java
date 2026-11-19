package db.monacgraph.so;

import db.monacgraph.serialize.ResultSubgraph;
import db.monacgraph.serialize.VsetResultSerializer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/** Cheapest OUT-directed path between two vertices; edge costs default to 1.0. */
public final class ShortestPathQueryBuilder {
    private final GraphTraversalSource g;
    private final Object sourceId;
    private final Object targetId;
    private Map<Edge, Double> costs = Collections.emptyMap();
    private int maxLength = Integer.MAX_VALUE;
    private long seq = 0;
    private List<Vertex> pathVertices = List.of();
    private List<Edge> pathEdges = List.of();

    public ShortestPathQueryBuilder(GraphTraversalSource g, Object sourceId, Object targetId) {
        this.g = g;
        this.sourceId = sourceId;
        this.targetId = targetId;
    }

    public ShortestPathQueryBuilder cost(Map<Edge, Double> edgeCosts) {
        this.costs = edgeCosts;
        return this;
    }

    public ShortestPathQueryBuilder maxLength(int l) {
        this.maxLength = l;
        return this;
    }

    public Set<Vertex> execute() {
        search();
        return new LinkedHashSet<>(pathVertices);
    }

    public Map<String, Object> executeForWeb() {
        search();
        return VsetResultSerializer.serializeSubgraphs(
                pathVertices.isEmpty() ? List.of() : List.of(ResultSubgraph.of(pathVertices, pathEdges)));
    }

    private void search() {
        Vertex s = g.V(sourceId).next();
        Vertex t = g.V(targetId).next();
        if (s.id().equals(t.id())) {
            pathVertices = List.of(s);
            return;
        }
        // unbounded: edge-heap Dijkstra; bounded: hops must be part of the state
        if (maxLength == Integer.MAX_VALUE) {
            dijkstra(s, t);
        } else {
            layered(s, t, maxLength);
        }
    }

    private double w(Edge e) {
        Double c = costs.get(e);
        return c == null ? 1.0 : c;
    }

    // ---------- unlimited hops: edge-driven Dijkstra ----------

    private static final class Hop implements Comparable<Hop> {
        final double cost;
        final Vertex from;
        final Vertex to;
        final Edge edge;
        final long seq;

        Hop(double cost, Vertex from, Vertex to, Edge edge, long seq) {
            this.cost = cost;
            this.from = from;
            this.to = to;
            this.edge = edge;
            this.seq = seq;
        }

        /** cost -> target id -> insertion order, so equal-cost results stay stable. */
        @Override
        public int compareTo(Hop o) {
            int c = Double.compare(cost, o.cost);
            if (c != 0) {
                return c;
            }
            c = Integer.compare(to.id().hashCode(), o.to.id().hashCode());
            return c != 0 ? c : Long.compare(seq, o.seq);
        }
    }

    private void dijkstra(Vertex s, Vertex t) {
        Map<Object, Double> dist = new HashMap<>();
        Map<Object, Edge> predEdge = new HashMap<>();
        Map<Object, Vertex> predFrom = new HashMap<>();
        dist.put(s.id(), 0.0);

        PriorityQueue<Hop> pq = new PriorityQueue<>();
        push(pq, s, 0.0);

        while (!pq.isEmpty()) {
            Hop h = pq.poll();
            if (dist.containsKey(h.to.id())) {
                continue;
            }
            dist.put(h.to.id(), h.cost);
            predEdge.put(h.to.id(), h.edge);
            predFrom.put(h.to.id(), h.from);
            if (h.to.id().equals(t.id())) {
                buildBack(t, s, predEdge, predFrom);
                return;
            }
            push(pq, h.to, h.cost);
        }
    }

    private void push(PriorityQueue<Hop> pq, Vertex v, double base) {
        Iterator<Edge> it = v.edges(Direction.OUT);
        while (it.hasNext()) {
            Edge e = it.next();
            pq.add(new Hop(base + w(e), v, e.inVertex(), e, seq++));
        }
    }

    private void buildBack(Vertex t, Vertex s,
                           Map<Object, Edge> predEdge, Map<Object, Vertex> predFrom) {
        LinkedList<Vertex> vs = new LinkedList<>();
        LinkedList<Edge> es = new LinkedList<>();
        Vertex cur = t;
        vs.addFirst(cur);
        while (!cur.id().equals(s.id())) {
            es.addFirst(predEdge.get(cur.id()));
            cur = predFrom.get(cur.id());
            vs.addFirst(cur);
        }
        pathVertices = vs;
        pathEdges = es;
    }

    // ---------- limited hops: layered DP ----------

    /**
     * One distance label per vertex is not enough once hops are limited: a cheap-but-long
     * route can block a legal short one, so keep dist[hop][vertex] instead.
     */
    private void layered(Vertex s, Vertex t, int L) {
        Map<Object, Double> cur = new HashMap<>();
        Map<Object, Vertex> at = new HashMap<>();
        cur.put(s.id(), 0.0);
        at.put(s.id(), s);

        List<Map<Object, Vertex>> fromL = new ArrayList<>();
        List<Map<Object, Edge>> edgeL = new ArrayList<>();
        double best = Double.POSITIVE_INFINITY;
        int bestHop = -1;

        for (int h = 1; h <= L && !cur.isEmpty(); h++) {
            Map<Object, Double> next = new HashMap<>();
            Map<Object, Vertex> from = new HashMap<>();
            Map<Object, Edge> via = new HashMap<>();

            List<Object> keys = new ArrayList<>(cur.keySet());
            keys.sort(ShortestPathQueryBuilder::compareIds);
            for (Object k : keys) {
                Vertex v = at.get(k);
                double base = cur.get(k);
                Iterator<Edge> it = v.edges(Direction.OUT);
                while (it.hasNext()) {
                    Edge e = it.next();
                    Vertex to = e.inVertex();
                    double nd = base + w(e);
                    Double old = next.get(to.id());
                    if (old == null || nd < old) {
                        next.put(to.id(), nd);
                        from.put(to.id(), v);
                        via.put(to.id(), e);
                        at.put(to.id(), to);
                    }
                }
            }

            Double dt = next.get(t.id());
            if (dt != null && dt < best) {
                best = dt;
                bestHop = h;
            }
            fromL.add(from);
            edgeL.add(via);
            cur = next;
        }

        if (bestHop < 0) {
            return;
        }
        LinkedList<Vertex> vs = new LinkedList<>();
        LinkedList<Edge> es = new LinkedList<>();
        Vertex cur2 = t;
        vs.addFirst(cur2);
        for (int h = bestHop; h >= 1; h--) {
            es.addFirst(edgeL.get(h - 1).get(cur2.id()));
            cur2 = fromL.get(h - 1).get(cur2.id());
            vs.addFirst(cur2);
        }
        pathVertices = vs;
        pathEdges = es;
    }

    private static int compareIds(Object a, Object b) {
        if (a instanceof Long && b instanceof Long) {
            return Long.compare((Long) a, (Long) b);
        }
        if (a instanceof Integer && b instanceof Integer) {
            return Integer.compare((Integer) a, (Integer) b);
        }
        return Integer.compare(a.hashCode(), b.hashCode());
    }
}
