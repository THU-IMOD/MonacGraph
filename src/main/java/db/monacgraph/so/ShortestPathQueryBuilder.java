package db.monacgraph.so;

import db.monacgraph.community.CommunityEdge;
import db.monacgraph.community.CommunityGraph;
import db.monacgraph.community.CommunityVertex;
import db.monacgraph.serialize.ResultSubgraph;
import db.monacgraph.serialize.VsetResultSerializer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

/** Cheapest OUT/IN/BOTH path between two vertices; edge costs default to 1.0. */
public final class ShortestPathQueryBuilder {
    private final GraphTraversalSource g;
    private final Object sourceId;
    private final Object targetId;
    private Map<Edge, Double> costs = Collections.emptyMap();
    private Direction direction = Direction.OUT;
    private int maxLength = Integer.MAX_VALUE;
    private List<Vertex> pathVertices = List.of();
    private List<Edge> pathEdges = List.of();

    private CommunityGraph cg;
    private long next = 0;
    private long seq = 0;
    private long s;
    private long t;
    // vertex handle -> [edgeHandle, otherEndpoint, edgeHandle, otherEndpoint, ...]
    private final Map<Long, List<Long>> adj = new HashMap<>();
    private final Map<Object, Long> vid = new HashMap<>();
    private final Map<Long, Vertex> vmap = new HashMap<>();
    private final Map<Object, Long> eid = new HashMap<>();
    private final Map<Long, Edge> emap = new HashMap<>();

    public ShortestPathQueryBuilder(GraphTraversalSource g, Object sourceId, Object targetId) {
        this.g = g;
        this.sourceId = sourceId;
        this.targetId = targetId;
    }

    public ShortestPathQueryBuilder cost(Map<Edge, Double> edgeCosts) {
        this.costs = edgeCosts;
        return this;
    }

    public ShortestPathQueryBuilder direction(Direction d) {
        this.direction = d;
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
        build();
        if (s == t) {
            pathVertices = List.of(vertex(s));
            return;
        }
        Map<Long, Double> c = new HashMap<>();
        for (Map.Entry<Edge, Double> e : costs.entrySet()) {
            c.put(handle(e.getKey()), e.getValue());
        }
        // unbounded: edge-heap Dijkstra; bounded: hop count must be part of the state
        if (maxLength == Integer.MAX_VALUE) {
            dijkstra(c);
        } else {
            layered(c, maxLength);
        }
    }

    private void build() {
        // One bulk getAllEdges() beats per-vertex neighbour lookups by ~180x:
        // get_neighbor() re-reads the adjacency of a vertex on every call.
        boolean out = direction != Direction.IN;
        boolean in = direction != Direction.OUT;
        if (g.getGraph() instanceof CommunityGraph) {
            cg = (CommunityGraph) g.getGraph();
            for (long e : cg.getJni().getAllEdges(cg.handle())) {
                long o = (e >>> 32) & 0xFFFFFFFFL;   // edge handle packs (out, in)
                long i = e & 0xFFFFFFFFL;
                if (out) {
                    link(o, e, i);
                }
                if (in) {
                    link(i, e, o);
                }
            }
        } else {
            Iterator<Edge> it = g.getGraph().edges();
            while (it.hasNext()) {
                Edge e = it.next();
                long eh = handle(e);
                long o = handle(e.outVertex());
                long i = handle(e.inVertex());
                if (out) {
                    link(o, eh, i);
                }
                if (in) {
                    link(i, eh, o);
                }
            }
        }
        s = handle(g.V(sourceId).next());
        t = handle(g.V(targetId).next());
    }

    private void link(long v, long e, long other) {
        List<Long> l = adj.computeIfAbsent(v, k -> new ArrayList<>());
        l.add(e);
        l.add(other);
    }

    /** Non-LSM graphs (unit tests) get synthetic handles assigned on first sight. */
    private long handle(Vertex v) {
        if (cg != null) {
            return ((CommunityVertex) v).handle();
        }
        Long h = vid.get(v.id());
        if (h == null) {
            h = next++;
            vid.put(v.id(), h);
            vmap.put(h, v);
        }
        return h;
    }

    private long handle(Edge e) {
        if (cg != null) {
            return ((CommunityEdge) e).handle();
        }
        Long h = eid.get(e.id());
        if (h == null) {
            h = next++;
            eid.put(e.id(), h);
            emap.put(h, e);
        }
        return h;
    }

    private Vertex vertex(long h) {
        return cg != null ? new CommunityVertex(cg, h) : vmap.get(h);
    }

    private Edge edge(long e) {
        return cg != null ? new CommunityEdge(cg, e) : emap.get(e);
    }

    private double weight(Map<Long, Double> c, long e) {
        Double w = c.get(e);
        return w == null ? 1.0 : w;
    }

    private static final class Hop implements Comparable<Hop> {
        final double cost;
        final long from;
        final long to;
        final long edge;
        final long seq;

        Hop(double cost, long from, long to, long edge, long seq) {
            this.cost = cost;
            this.from = from;
            this.to = to;
            this.edge = edge;
            this.seq = seq;
        }

        /** cost -> target handle -> insertion order, so equal-cost runs stay stable. */
        @Override
        public int compareTo(Hop o) {
            int c = Double.compare(cost, o.cost);
            if (c != 0) {
                return c;
            }
            int i = Long.compare(to, o.to);
            return i != 0 ? i : Long.compare(seq, o.seq);
        }
    }

    /** Edge-driven Dijkstra: pop the cheapest edge, its head vertex is final. */
    private void dijkstra(Map<Long, Double> c) {
        Map<Long, Double> dist = new HashMap<>();
        Map<Long, Long> prevEdge = new HashMap<>();
        Map<Long, Long> prevVertex = new HashMap<>();
        dist.put(s, 0.0);

        PriorityQueue<Hop> pq = new PriorityQueue<>();
        expand(pq, c, s, 0.0);
        while (!pq.isEmpty()) {
            Hop h = pq.poll();
            if (dist.containsKey(h.to)) {
                continue;
            }
            dist.put(h.to, h.cost);
            prevEdge.put(h.to, h.edge);
            prevVertex.put(h.to, h.from);
            if (h.to == t) {
                List<Long> vs = new ArrayList<>();
                List<Long> es = new ArrayList<>();
                for (long cur = t; ; cur = prevVertex.get(cur)) {
                    vs.add(0, cur);
                    if (cur == s) {
                        break;
                    }
                    es.add(0, prevEdge.get(cur));
                }
                setPath(vs, es);
                return;
            }
            expand(pq, c, h.to, h.cost);
        }
    }

    private void expand(PriorityQueue<Hop> pq, Map<Long, Double> c, long v, double base) {
        List<Long> l = adj.get(v);
        if (l == null) {
            return;
        }
        for (int i = 0; i + 1 < l.size(); i += 2) {
            pq.add(new Hop(base + weight(c, l.get(i)), v, l.get(i + 1), l.get(i), seq++));
        }
    }

    /**
     * Hop-bounded search as layered DP over dist[hop][vertex]. One label per vertex is
     * not enough: a cheap-but-long route would block a legal shorter-hop route.
     */
    private void layered(Map<Long, Double> c, int L) {
        Map<Long, Double> cur = new HashMap<>();
        cur.put(s, 0.0);
        List<Map<Long, Long>> fromAt = new ArrayList<>();
        List<Map<Long, Long>> edgeAt = new ArrayList<>();
        double best = Double.POSITIVE_INFINITY;
        int bestHop = -1;

        for (int h = 1; h <= L && !cur.isEmpty(); h++) {
            Map<Long, Double> next = new HashMap<>();
            Map<Long, Long> from = new HashMap<>();
            Map<Long, Long> via = new HashMap<>();
            List<Long> keys = new ArrayList<>(cur.keySet());
            Collections.sort(keys);
            for (long v : keys) {
                List<Long> l = adj.get(v);
                if (l == null) {
                    continue;
                }
                double base = cur.get(v);
                for (int i = 0; i + 1 < l.size(); i += 2) {
                    long to = l.get(i + 1);
                    double nd = base + weight(c, l.get(i));
                    if (nd < next.getOrDefault(to, Double.POSITIVE_INFINITY)) {
                        next.put(to, nd);
                        from.put(to, v);
                        via.put(to, l.get(i));
                    }
                }
            }
            Double dt = next.get(t);
            if (dt != null && dt < best) {
                best = dt;
                bestHop = h;
            }
            fromAt.add(from);
            edgeAt.add(via);
            cur = next;
        }
        if (bestHop < 0) {
            return;
        }
        List<Long> vs = new ArrayList<>();
        List<Long> es = new ArrayList<>();
        long cv = t;
        vs.add(0, cv);
        for (int h = bestHop; h >= 1; h--) {
            es.add(0, edgeAt.get(h - 1).get(cv));
            cv = fromAt.get(h - 1).get(cv);
            vs.add(0, cv);
        }
        setPath(vs, es);
    }

    private void setPath(List<Long> hv, List<Long> he) {
        List<Vertex> vs = new ArrayList<>(hv.size());
        for (long h : hv) {
            vs.add(vertex(h));
        }
        List<Edge> es = new ArrayList<>(he.size());
        for (long h : he) {
            es.add(edge(h));
        }
        pathVertices = vs;
        pathEdges = es;
    }
}
