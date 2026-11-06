# MonacGraph - Second-Order Graph Query System

> A modern web interface for MonacGraph with real-time graph visualization and second-order logic support

[![Vue 3](https://img.shields.io/badge/Vue-3.x-4FC08D?logo=vue.js)](https://vuejs.org/)
[![Element Plus](https://img.shields.io/badge/Element_Plus-2.x-409EFF)](https://element-plus.org/)
[![Cytoscape.js](https://img.shields.io/badge/Cytoscape.js-3.x-FFAA00)](https://js.cytoscape.org/)

## ✨ Features

### 🎨 Modern UI
- **Glassmorphism Design** - Beautiful frosted glass effects with backdrop filters
- **Responsive Layout** - Adaptive interface for different screen sizes
- **Dark Theme Compatible** - Comfortable viewing experience
- **Smooth Animations** - Polished transitions and interactions

### 📊 Graph Visualization
- **Interactive Graph Display** - Pan, zoom, and explore graph structures
- **Multiple Layout Algorithms**
  - Cose-Bilkent (force-directed)
  - Circle (circular layout)
  - Grid (matrix layout)
- **Node & Edge Properties** - Hover tooltips showing all properties
- **Smart Node Sizing** - Dynamic sizing based on graph size
- **Edge Labels** - Display edge types and key properties (weight, since, year)

### 🔍 Query Features
- **Query Editor** - Syntax highlighting and Ctrl+Enter execution
- **Query History** - Track and reuse previous queries
- **Example Queries** - Built-in templates for common patterns
- **Result Views** - Table, JSON, and visualization modes

### 🎯 Second-Order Logic
- **Forall (∀) Quantifier** - Universal quantification
- **Exist (∃) Quantifier** - Existential quantification
- **Vset Queries** - Find all vertex sets satisfying conditions
- **Vset Browser** - Interactive browsing of result subsets with navigation

### 🧮 Graph Algorithms
- **WCC (Weakly Connected Components)** - Find all weakly connected components
- **SCC (Strongly Connected Components)** - Find all strongly connected components  
- **Community Detection** - Discover communities using LSM-Communities algorithm
- **BFS (Breadth-First Search)** - Find all vertices reachable from a starting vertex

### 🛠️ Data Management
- **Upload Graph** - Import `.graph` topology with optional CSV/JSON vertex and edge properties
- **Create Test Data** - Generate sample graph (Alice, Bob, Charlie, David)
- **Refresh Graph** - Reload current graph state
- **Clear Graph** - Remove all vertices and edges

## 📋 Prerequisites

- **Node.js** >= 16.0.0
- **npm** or **yarn**
- **MonacGraph Server** - Gremlin Server with second-order extensions
  - Default: `ws://localhost:8182/gremlin`

## 🚀 Quick Start

### 1. Install Dependencies

```bash
npm install
```

### 2. Start Development Server

```bash
npm run dev
```

The application will open at `http://localhost:5173`

### 3. Connect to Server

1. Click the **"Connected"** indicator (or **"Disconnected"**)
2. Enter server URL: `ws://localhost:8182/gremlin`
3. Click **"Connect"**
4. Connection status will turn green when successful

### 4. Try Example Queries

Click the **"Examples"** tab to explore:
- **Basic Queries**: Get all vertices/edges, count statistics
- **Second-Order Logic**: Forall/exist quantifier queries
- **Vset Queries**: Find all cliques and vertex subsets
- **Graph Algorithms**: WCC, SCC, Community detection, BFS

## 📁 Project Structure

```
vue-demo/
├── src/
│   ├── components/
│   │   ├── ConnectionDialog.vue      # Server connection UI
│   │   ├── QueryEditor.vue           # Query input with history
│   │   ├── GraphVisualization.vue    # Cytoscape.js visualization
│   │   ├── ResultsPanel.vue          # Query results display
│   │   ├── VsetBrowser.vue           # Vset result browser
│   │   └── NodeDetailPanel.vue       # Node property inspector
│   ├── services/
│   │   └── gremlinClient.js          # WebSocket client with GraphSON parser
│   ├── store/
│   │   └── graphStore.js             # Pinia state management
│   ├── App.vue                       # Main application
│   ├── main.js                       # Entry point
│   └── style.css                     # Global styles
├── public/
├── package.json
├── vite.config.js
└── README.md
```

## 🎮 Usage Guide

### Basic Graph Queries

```javascript
// Get all vertices
g.V().valueMap(true).toList()

// Get all edges
g.E().valueMap(true).toList()

// Traversal query
g.V().has('name', 'Alice').out('knows').values('name')
```

### Second-Order Logic Queries

```javascript
// Check if everyone knows someone
g.SecondOrder()
  .forall('x')
  .exist('y')
  .filter('g.V(x).out("knows").is(y)')
  .execute()

// Check if there exists someone who knows everyone
g.SecondOrder()
  .exist('x')
  .forall('y')
  .filter('g.V(x).out("knows").is(y) || g.V(x).is(y)')
  .execute()
```

### Vset Queries (Find All Subsets)

```javascript
// Find all cliques (complete subgraphs)
g.Vset()
  .forall('x')
  .forall('y')
  .filter('g.V(x).out("knows").is(y) || g.V(x).is(y)')
  .executeForWeb()

// Find all independent sets
g.Vset()
  .forall('x')
  .forall('y')
  .filter('g.V(x).bothE().otherV().is(y).count().is(0) || g.V(x).is(y)')
  .executeForWeb()

// Find all cliques with size > 1
g.Vset()
  .forall('x')
  .forall('y')
  .filter('g.V(x).bothE().otherV().is(y) || g.V(x).is(y)')
  .having('size > 1')
  .executeForWeb()
```

### Graph Algorithms

```javascript
// Find Weakly Connected Components
g.WCC().executeForWeb()
// Returns all WCC as Vset - each subset is one component

// Find Strongly Connected Components
g.SCC().executeForWeb()
// Returns all SCC as Vset - useful for directed graphs

// Detect Communities (LSM-Communities)
g.Community().executeForWeb()
// Returns all communities as Vset - groups of densely connected vertices

// Breadth-First Search from vertex 1
g.BFS(1).executeForWeb()
// Returns all vertices reachable from vertex 1

// BFS from a specific vertex ID
g.BFS(vertexId).executeForWeb()
// Replace vertexId with actual vertex ID (e.g., g.BFS(42).executeForWeb())
```

**Common Use Cases**:
- **WCC**: Identify disconnected parts of an undirected graph (e.g., separate social circles)
- **SCC**: Find strongly connected regions in directed graphs (e.g., web page link cycles)
- **Community**: Discover natural groupings in social networks
- **BFS**: Check reachability, shortest paths, or connected neighbors

**Result Display**: All algorithms except BFS return results in Vset format, which can be browsed using the Vset Browser panel at the bottom of the screen.

### Graph Visualization Controls

- **Mouse Wheel** - Zoom in/out
- **Click + Drag** - Pan the graph
- **Click Node** - View node details in side panel
- **Hover Node/Edge** - See properties in tooltip
- **Fit Button** - Auto-fit graph to viewport
- **Reset Button** - Reset zoom and center graph
- **Layout Button** - Cycle through layout algorithms

### Vset Result Navigation

When a Vset query or graph algorithm (WCC/SCC/Community) returns multiple subsets:
1. Use **← →** arrows to navigate between subsets
2. **Subset indicator** shows current position (e.g., "13 / 32")
3. **Vertex list** on the right shows subset members with their properties
4. **Fit/Reset** buttons adjust the subgraph view
5. **Layout button** cycles through different arrangements (cose-bilkent, circle, grid, concentric)

**Note**: WCC, SCC, and Community algorithms return their results as Vset format, allowing you to browse through each component or community individually.

## 🏗️ Build for Production

```bash
# Build for production
npm run build

# Preview production build locally
npm run preview
```

Built files will be in the `dist/` directory.

## 🔧 Configuration

### Vite Config (`vite.config.js`)

```javascript
export default defineConfig({
  server: {
    port: 5173,
    host: true
  },
  plugins: [vue()]
})
```

### Gremlin Server Connection

Edit connection settings in the UI or modify defaults in `src/services/gremlinClient.js`:

```javascript
const defaultConfig = {
  url: 'ws://localhost:8182/gremlin',
  traversalSource: 'g'
}
```

## 🐛 Troubleshooting

### Connection Issues
- ✅ Ensure MonacGraph server is running
- ✅ Check server URL (should start with `ws://`)
- ✅ Verify port 8182 is accessible
- ✅ Check browser console for WebSocket errors

### Graph Not Displaying
- ✅ Click **"Refresh Graph"** to reload
- ✅ Try **"Fit"** button to auto-center
- ✅ Check if query returned data (see Results tab)
- ✅ Hard refresh browser: Ctrl+Shift+R

### Nodes Clipped at Edges
- ✅ Click **"Fit"** or **"Reset"** button
- ✅ Zoom out using mouse wheel
- ✅ Switch to different layout (circle/grid)

### Vset Results Not Showing
- ✅ Ensure query uses `g.Vset()` not `g.secondOrder()`
- ✅ Check that query uses `.execute()` at the end
- ✅ Verify result type in Results panel (should be VsetResult)

## 📚 Resources

### Documentation
- **Gremlin Documentation**: https://tinkerpop.apache.org/docs/current/reference/
- **Cytoscape.js**: https://js.cytoscape.org/
- **Element Plus**: https://element-plus.org/
- **Vue 3**: https://vuejs.org/

### Graph Algorithms
- **WCC**: Finds maximal subgraphs where any two vertices are connected by a path (ignoring edge direction)
- **SCC**: Finds maximal subgraphs where any two vertices are mutually reachable (considering edge direction)
- **LSM-Communities**: Label-based community detection algorithm for identifying densely connected groups
- **BFS**: Explores graph level by level from a starting vertex to find all reachable vertices

## 📧 Contact

**Email**: jinyt23@mails.tsinghua.edu.cn

**Issues**: Please report bugs or feature requests via GitHub Issues
