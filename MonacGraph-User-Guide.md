# MonacGraph User Guide

MonacGraph is a graph database system built on the Apache TinkerPop framework. It extends standard Gremlin with **SO-Gremlin** — a Monadic Second-Order Logic (MSOL) query language that supports quantification over vertex sets, enabling expressive set-level pattern matching that is impossible in standard first-order graph query languages. The storage backend is **LSM-Community**, a community-structure-aware LSM-tree engine written in Rust.

Source repository: https://github.com/THU-IMOD/MonacGraph

---

## Table of Contents

1. [Opening a Graph](#1-opening-a-graph)
2. [Loading Data](#2-loading-data)
3. [Standard Gremlin Queries](#3-standard-gremlin-queries)
4. [SO-Gremlin Overview](#4-so-gremlin-overview)
5. [Scope Operators](#5-scope-operators)
6. [Quantifier Clause](#6-quantifier-clause)
7. [Filter Clause](#7-filter-clause)
8. [Having Clause](#8-having-clause)
9. [Complete SO-Gremlin Examples](#9-complete-so-gremlin-examples)
10. [SO-Gremlin Syntax Reference](#10-so-gremlin-syntax-reference)
11. [Running via Gremlin Server](#11-running-via-gremlin-server)

---

## 1. Opening a Graph

```java
import db.monacgraph.community.CommunityGraph;
import db.monacgraph.so.SecondOrderTraversalSource;

// Open (or create) a graph stored under the given name/path
CommunityGraph graph = CommunityGraph.open("example");

// Obtain a traversal source with SO-Gremlin support
SecondOrderTraversalSource g = graph.traversal(SecondOrderTraversalSource.class);

// Always close when done
graph.close();
```

---

## 2. Loading Data

MonacGraph accepts graph topology files in `.graph` format and property files in **JSON** or **CSV** format.

### Graph topology file (`.graph`)

```
t <vertex_count> <edge_count>
v <id> <label_int> <degree>
...
e <src_id> <dst_id>
...
```

Example (`example.graph`, 13 vertices, 20 edges):
```
t 13 20
v 0 0 2
v 1 0 3
...
e 0 2
e 1 0
...
```

### Vertex property file

**JSON format** (`exampleVertexProperty.json`):
```json
[
  {
    "vertex": 0,
    "label": "person",
    "name": "Alice",
    "age": 30,
    "city": "New York"
  },
  {
    "vertex": 1,
    "label": "person",
    "name": "Bob",
    "age": 25,
    "city": "Los Angeles"
  }
]
```

Each object must contain a `"vertex"` field (integer ID) and a `"label"` field. All other fields are stored as vertex properties.

**CSV format** (`exampleVertexProperty.csv`):

```
id,label,name,age,city
0,person,Alice,30,New York
1,person,Bob,25,Los Angeles
```

### Edge property file

**JSON format** (`exampleEdgeProperty.json`):

```json
[
  {
    "outVertex": 0,
    "inVertex": 2,
    "label": "relationship",
    "type": "friend",
    "since": 2014
  },
  {
    "outVertex": 1,
    "inVertex": 0,
    "label": "relationship",
    "type": "colleague",
    "since": 2016
  }
]
```

Each object must contain `"outVertex"` and `"inVertex"` (integer IDs) and a `"label"` field. All other fields are stored as edge properties.

**CSV format** (`exampleEdgeProperty.csv`):
```
src,dst,label,type,since
0,2,relationship,friend,2014
1,0,relationship,colleague,2016
```

### Loading in code

```java
graph.loadVertexProperty("exampleVertexProperty.json");  // JSON
graph.loadEdgeProperty("exampleEdgeProperty.json");      // JSON

// or CSV equivalents
graph.loadVertexProperty("exampleVertexProperty.csv");
graph.loadEdgeProperty("exampleEdgeProperty.csv");
```

### Checking counts

```java
System.out.println("Vertices: " + g.V().count().next());  // e.g. 13
System.out.println("Edges:    " + g.E().count().next());  // e.g. 20
```

---

## 3. Standard Gremlin Queries

`SecondOrderTraversalSource` is fully compatible with Apache TinkerPop. All standard Gremlin traversal steps work as expected.

```java
// Filter by label
g.V().hasLabel("person").toList();

// Filter by property
g.V().has("age", P.gt(28)).toList();

// Traverse edges
g.V(0).out().toList();       // outgoing neighbors
g.V(0).in().toList();        // incoming neighbors
g.V(0).both().toList();      // all neighbors

// Get property values
g.V(0).values("name").next();   // → "Alice"
g.V(0).valueMap().next();       // → {name=[Alice], age=[30], city=[New York]}

// Aggregation
g.V().hasLabel("person").values("age").mean().next();
g.V().groupCount().by("label").next();
```

---

## 4. SO-Gremlin Overview

SO-Gremlin extends Gremlin with **Monadic Second-Order Logic (MSOL)**, which allows quantification over *vertex sets* rather than individual vertices. This enables queries such as:

> *"Find all communities where there exists a member who has a direct relationship with every other member."*

which cannot be expressed in standard first-order Gremlin.

All SO-Gremlin queries follow **prenex normal form** — quantifiers are declared first, followed by the filter clause:

```java
g.<scope>()
 .quantifier("var1")
 .quantifier("var2")
 ...
 .filter("first_order_clause")
 .having("subset_constraint")
 .execute()
```

---

## 5. Scope Operators

The scope operator defines the **search space** of candidate vertex sets. Choosing the right scope dramatically affects performance.

| Operator | Search Space | Return Type | Notes |
|---|---|---|---|
| `Vset()` | All vertex subsets (2^n) | `Set<Set<Vertex>>` | General-purpose; exponential search space |
| `Community()` | Precomputed community partitions (C ≪ 2^n) | `Set<Set<Vertex>>` | Fastest for community queries |
| `WCC()` | Weakly connected components | `Set<Set<Vertex>>` | Ignores edge direction |
| `SCC()` | Strongly connected components | `Set<Set<Vertex>>` | Follows edge direction |
| `SecondOrder()` | Entire graph as one object | `Boolean` | Returns true/false for the whole graph |
| `Bfs(x)` | BFS-reachable vertices from x | `Set<Vertex>` | Returns a flat set, not a set of sets |
| `Subgraph().addV().addE()` | Embeddings of a query graph pattern | `List<List<Vertex>>` | Requires C++ engine; see below |

### Vset() — all vertex subsets

Use when you need to search over arbitrary subsets with no pre-existing structure.

```java
Set<Set<Vertex>> results = g.Vset()
    .forall("x")
    .forall("y")
    .filter("g.V(x).bothE().otherV().is(y) || g.V(x).is(y)")
    .having("size > 1")
    .execute();
```

### Community() — precomputed communities

LSM-Community stores precomputed partitions and returns them directly, avoiding runtime community detection. Use when queries target community structure.

```java
// Find the community containing a specific vertex
Set<Set<Vertex>> results = g.Community()
    .exist("x")
    .filter("g.V(x).hasId(42)")
    .execute();
```

### WCC() — weakly connected components

Searches among weakly connected components (edge direction ignored).

```java
Set<Set<Vertex>> results = g.WCC()
    .forall("x")
    .forall("y")
    .filter("x == y || (int)x.value('age') + (int)y.value('age') > 50")
    .having("size >= 3")
    .execute();
```

### SCC() — strongly connected components

Searches among strongly connected components (following edge direction). Useful for cycle analysis in directed graphs.

```java
Set<Set<Vertex>> results = g.SCC()
    .exist("x")
    .forall("y")
    .filter("g.V(x).bothE().otherV().is(y) || g.V(x).is(y)")
    .having("size > 1")
    .execute();
```

### SecondOrder() — whole-graph boolean query

Treats the entire graph as a single candidate. Returns a `Boolean`.

```java
Boolean result = (Boolean) g.SecondOrder()
    .exist("x")
    .forall("y")
    .filter("g.V(x).bothE().otherV().is(y) || g.V(x).is(y)")
    .execute();
```

### Bfs(x) — BFS reachable set

Returns the flat set of vertices reachable from a given starting vertex via BFS. Returns `Set<Vertex>`, not `Set<Set<Vertex>>`.

```java
Set<Vertex> reachable = g.Bfs(g.V(0).next()).execute();
```

### Subgraph().addV().addE() — subgraph pattern matching

Finds all embeddings of a structural query graph pattern in the data graph, filtered by an MSOL formula over the matched vertices. The query graph can be defined in two ways: **inline** via `.addV()` / `.addE()`, or **from file** via `.fromFile()`. The two modes are mutually exclusive within one query.

> **Prerequisite:** The C++ subgraph matching engine must be compiled and its native library placed under `resources/storage/<os>/`. See README.md for build instructions. If the library is absent, `SubgraphJNI.isAvailable()` returns `false` and calling `.execute()` throws `UnsupportedOperationException`.

The query graph topology is declared with `.addV(id, label)` and `.addE(u, v)`, where IDs are local to the query pattern and labels must match labels present in the data graph. After the topology is defined, quantifiers, filter, and having clauses follow the same syntax as all other scope operators.

**Return type:** `List<List<Vertex>>`. Each inner list is one match; its i-th element is the data vertex matched to query vertex i. Different assignments of the same data vertices to different query vertices are treated as distinct matches (automorphic duplicates). Use `.unique()` to collapse them.

```java
// Triangle pattern: three "person" vertices, all mutually connected
//
//   0
//  / \
// 1 - 2
//
List<List<Vertex>> results = g.Subgraph()
    .addV(0, "person")
    .addV(1, "person")
    .addV(2, "person")
    .addE(0, 1)
    .addE(1, 2)
    .addE(0, 2)
    .forall("x")
    .forall("y")
    .filter("x == y" +
            " || ((int)x.value('age') + (int)y.value('age') >= 60)" +
            " || (Math.abs((int)x.value('age') - (int)y.value('age')) < 4)")
    .execute();
```

#### Loading the query graph from file — `.fromFile(graphFile, labelMapFile)`

Instead of calling `.addV()` / `.addE()` manually, you can load the query graph topology from a `.graph` file and a JSON label map file, both resolved under the `data/` directory.

**Label map file** (`data/labelMap.json`) — maps integer label IDs in the `.graph` file to vertex label strings:
```json
{
  "0": "person",
  "1": "paper"
}
```

```java
List<List<Vertex>> results = g.Subgraph()
    .fromFile("query.graph", "labelMap.json")   // loads data/query.graph and data/labelMap.json
    .forall("x")
    .forall("y")
    .filter("x == y || Math.abs(x.value('identity') - y.value('identity')) > 100")
    .execute();
```

#### Deduplication — `.unique()`

By default, every distinct mapping of query vertices to data vertices is returned as a separate result. A symmetric query graph (e.g. a triangle) produces `qn!` results per matched subgraph — 6 per triangle for a 3-vertex query. Call `.unique()` to keep only one representative per distinct (vertex set, edge set) pair.

```java
List<List<Vertex>> results = g.Subgraph()
    .addV(0, "person").addV(1, "person").addV(2, "person")
    .addE(0, 1).addE(1, 2).addE(0, 2)
    .forall("x").forall("y")
    .filter("x == y" +
            " || ((int)x.value('age') + (int)y.value('age') >= 60)" +
            " || (Math.abs((int)x.value('age') - (int)y.value('age')) < 4)")
    .unique()       // collapses 24 automorphic duplicates → 4 unique triangles
    .execute();
```

Other supported patterns:

```java
// Path of length 2: 0 → 1 → 2
.addV(0,"person").addV(1,"person").addV(2,"person")
.addE(0,1).addE(1,2)

// Star: center 0 connected to three leaves
.addV(0,"person").addV(1,"person").addV(2,"person").addV(3,"person")
.addE(0,1).addE(0,2).addE(0,3)
```

Checking availability at runtime:

```java
if (!SubgraphJNI.isAvailable()) {
    System.out.println("Subgraph engine not available. " +
                       "Build the C++ engine following README.md instructions.");
}
```

---

## 6. Quantifier Clause

Quantifiers declare the variables that range over the candidate vertex set.

```java
.forall("x")   // Universal quantifier: ∀x ∈ candidate set
.exist("x")    // Existential quantifier: ∃x ∈ candidate set
```

Quantifiers are chained in order and compose as nested quantifiers in prenex normal form:

```java
.forall("x").forall("y")   // ∀x ∀y — filter must hold for every pair
.exist("x").forall("y")    // ∃x ∀y — there exists x such that for all y the filter holds
.forall("x").exist("y")    // ∀x ∃y — for every x, some y satisfies the filter
```

A candidate subset S is accepted if and only if the composed quantifier formula holds. For example, with `∀x ∀y: phi(x,y)`, every pair from S must satisfy `phi`.

---

## 7. Filter Clause

The `.filter(String)` argument is a **Groovy boolean expression** evaluated for each instantiation of the quantified variables. Bound variables are **`Vertex` objects**. The filter supports `!` (NOT), `&&` (AND), `||` (OR), and standard Gremlin traversals via `g`. It cannot nest second-order quantifiers or aggregate over entire subsets.

```java
// Property access — cast explicitly for arithmetic
(int) x.value("age")
(double) x.value("score")
x.value("name")                        // String, no cast needed

// Identity check
x == y

// Connectivity via nested Gremlin
g.V(x).bothE().otherV().is(y)          // any-direction edge between x and y
g.V(x).out().toList().contains(y)      // outgoing edge from x to y

// Vertex ID check
g.V(x).hasId(2649)

// Arithmetic
Math.abs((int)x.value("age") - (int)y.value("age")) < 5
(int)x.value("age") + (int)y.value("age") >= 60

// String comparison
x.value("city") == y.value("city")
```

> **Tip:** Use single quotes inside the filter string for Groovy string literals, to avoid conflicts with Java's double-quote string delimiter.

---

## 8. Having Clause

`.having(String)` filters candidate subsets by cardinality after MSOL verification. The variable `size` refers to the number of vertices in the candidate set.

```java
.having("size > 1")
.having("size >= 3")
.having("size == 5")
.having("size >= 3 && size <= 10")
```

---

## 9. Complete SO-Gremlin Examples

### Example 1: Clique detection (Vset)

Find all vertex subsets of size > 1 where every pair of distinct vertices is connected:

```java
Set<Set<Vertex>> results = g.Vset()
    .forall("x")
    .forall("y")
    .filter("g.V(x).bothE().otherV().is(y) || g.V(x).is(y)")
    .having("size > 1")
    .execute();
```

### Example 2: Community of a specific vertex (Community)

```java
Set<Set<Vertex>> results = g.Community()
    .exist("x")
    .filter("g.V(x).hasId(42)")
    .execute();
```

### Example 3: SCC citation cycle analysis (SCC)

Find SCCs containing a hub vertex directly connected to every other member:

```java
Set<Set<Vertex>> results = g.SCC()
    .exist("x")
    .forall("y")
    .filter("g.V(x).bothE().otherV().is(y) || g.V(x).is(y)")
    .having("size > 1")
    .execute();
```

### Example 4: WCC age constraint (WCC)

Find weakly connected components where every pair has combined age > 50:

```java
Set<Set<Vertex>> results = g.WCC()
    .forall("x")
    .forall("y")
    .filter("x == y || (int)x.value('age') + (int)y.value('age') > 50")
    .execute();
```

### Example 5: Whole-graph hub check (SecondOrder)

```java
Boolean hasHub = (Boolean) g.SecondOrder()
    .exist("x")
    .forall("y")
    .filter("g.V(x).bothE().otherV().is(y) || g.V(x).is(y)")
    .execute();
```

### Example 6: BFS reachability (Bfs)

```java
Set<Vertex> reachable = g.Bfs(g.V(0).next()).execute();
reachable.forEach(v -> System.out.println(v.value("name")));
```

### Example 7: Triangle matching with age constraint (Subgraph)

Using the example dataset (13 persons, ages 22–40). Without `.unique()`, each triangle produces 3! = 6 automorphic matches:

```java
List<List<Vertex>> results = g.Subgraph()
    .addV(0, "person").addV(1, "person").addV(2, "person")
    .addE(0, 1).addE(1, 2).addE(0, 2)
    .forall("x")
    .forall("y")
    .filter("x == y" +
            " || ((int)x.value('age') + (int)y.value('age') >= 60)" +
            " || (Math.abs((int)x.value('age') - (int)y.value('age')) < 4)")
    .unique()   // collapse 24 → 4 unique triangles
    .execute();
// Expected: {Alice,Charlie,David}, {Bob,Charlie,David},
//           {Henry,Irene,Jack},    {Henry,Jack,Karen}
```

### Example 8: Subgraph matching from file (Subgraph)

```java
List<List<Vertex>> results = g.Subgraph()
    .fromFile("query.graph", "labelMap.json")
    .forall("x")
    .forall("y")
    .filter("x == y || Math.abs(x.value('identity') - y.value('identity')) > 100")
    .unique()
    .execute();
```

---

## 10. SO-Gremlin Syntax Reference

### Scope operator summary

| Operator | Search Space | Return Type |
|---|---|---|
| `g.Vset()` | All vertex subsets (2^n) | `Set<Set<Vertex>>` |
| `g.Community()` | Precomputed community partitions | `Set<Set<Vertex>>` |
| `g.WCC()` | Weakly connected components | `Set<Set<Vertex>>` |
| `g.SCC()` | Strongly connected components | `Set<Set<Vertex>>` |
| `g.SecondOrder()` | Entire graph | `Boolean` |
| `g.Bfs(v)` | BFS-reachable from vertex v | `Set<Vertex>` |
| `g.Subgraph().addV().addE()` | Subgraph pattern embeddings | `List<List<Vertex>>` |
| `g.Subgraph().fromFile(g, lm)` | Subgraph pattern embeddings (from file) | `List<List<Vertex>>` |

### Builder method summary

| Method | Description |
|---|---|
| `.forall("x")` | Declare universal quantifier ∀x |
| `.exist("x")` | Declare existential quantifier ∃x |
| `.filter("expr")` | Groovy boolean expression over bound variables |
| `.having("size > N")` | Cardinality constraint on matched set |
| `.execute()` | Run query and return results |
| `.addV(id, label)` | (Subgraph only) Declare query vertex inline |
| `.addE(u, v)` | (Subgraph only) Declare query edge inline |
| `.fromFile(graphFile, labelMapFile)` | (Subgraph only) Load query graph topology from `data/` directory; mutually exclusive with `.addV()` / `.addE()` |
| `.unique()` | (Subgraph only) Deduplicate matches that share the same vertex set and edge set; collapses automorphic duplicates |

### Filter expression context

| Name | Type | Description |
|---|---|---|
| `x`, `y`, ... | `Vertex` | Variables bound by quantifier declarations |
| `g` | `GraphTraversalSource` | Live traversal source for nested Gremlin |

### Property type casting in filters

| Property value | Java type | Cast in filter |
|---|---|---|
| Integer | `Integer` | `(int)x.value('age')` |
| Decimal | `Double` | `(double)x.value('score')` |
| String | `String` | `x.value('name')` — no cast needed |
| Boolean | `Boolean` | `(boolean)x.value('active')` |

---

## 11. Running via Gremlin Server

### Start the server

```bash
java -cp target/Gremmunity-1.0-SNAPSHOT.jar db.monacgraph.app.MonacGraphServer
```

### Connect via HTTP

```bash
curl -X POST http://localhost:8182 \
     -H "Content-Type: application/json" \
     -d '{"gremlin": "g.V().count()"}'
```

### Connect from Java (TinkerPop driver)

```java
Cluster cluster = Cluster.build("localhost").port(8182).create();
Client client = cluster.connect();
ResultSet results = client.submit("g.V().hasLabel('person').values('name')");
results.stream().forEach(r -> System.out.println(r.getString()));
cluster.close();
```

### Web visualization interface

A browser-based interface (built with Vite and Vue.js) is included in `web-client/`. It provides an interactive Query Editor, predefined SO-Gremlin query templates in the Examples Tab, and real-time graph visualization using Cytoscape.js. Clicking "Use This Query" on any template populates the editor; clicking "Execute" runs the query and renders matched subsets visually.

```bash
cd web-client
npm install
npm run dev
# Open http://localhost:5173 in your browser
```
