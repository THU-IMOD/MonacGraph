package db.monacgraph.so;

import db.monacgraph.utils.KDimensionalArray;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;

import java.util.*;

/**
 * Core executor for second-order logic queries on graph data using Gremlin and Groovy evaluation.
 *
 * ★ 性能优化说明：
 *   1. 在 secondOrderQuery / VsetQuery / CommunityQuery 入口处，将 groovyQuery 字符串
 *      一次性解析为 LogicalNode AST，叶子节点（LeafNode）在解析时同步完成 Groovy 脚本编译。
 *   2. Preconditioning → calcSecondOrder 的 n^k 次迭代直接复用该 AST，
 *      每次求值只做树遍历 + script.run()，零字符串解析、零重复编译。
 *   3. aggregationQuery 同样在入口处编译为 Script，循环内仅替换 Binding。
 */
@SuppressWarnings("all")
public class GroovyGremlinQueryExecutor {

    /** 唯一静态字段：仅用于 parse（parse 本身无副作用，可安全共享） */
    private static final GroovyShell SHARED_SHELL = new GroovyShell();

    // =========================================================================
    // AST 节点定义
    // =========================================================================

    /** 逻辑表达式 AST 节点基类 */
    public abstract static class LogicalNode {
        /** 用当前变量绑定对 AST 求值 */
        abstract boolean evaluate(Map<String, Object> variables);
    }

    /** OR 节点：子节点间短路求值 */
    public static class OrNode extends LogicalNode {
        final List<LogicalNode> children;
        OrNode(List<LogicalNode> children) { this.children = children; }

        @Override
        boolean evaluate(Map<String, Object> variables) {
            for (LogicalNode child : children) {
                if (child.evaluate(variables)) return true;
            }
            return false;
        }
    }

    /** AND 节点：子节点间短路求值 */
    public static class AndNode extends LogicalNode {
        final List<LogicalNode> children;
        AndNode(List<LogicalNode> children) { this.children = children; }

        @Override
        boolean evaluate(Map<String, Object> variables) {
            for (LogicalNode child : children) {
                if (!child.evaluate(variables)) return false;
            }
            return true;
        }
    }

    /** NOT 节点 */
    public static class NotNode extends LogicalNode {
        final LogicalNode child;
        NotNode(LogicalNode child) { this.child = child; }

        @Override
        boolean evaluate(Map<String, Object> variables) {
            return !child.evaluate(variables);
        }
    }

    /**
     * 叶子节点：持有原始表达式 + 编译好的 Script。
     * Script 在 AST 构建阶段一次性编译，后续求值只做 script.run()。
     */
    public static class LeafNode extends LogicalNode {
        final String expression;
        final Script script;           // null 仅当 expression 为 "true"/"false"
        final Boolean constantValue;   // 仅 "true"/"false" 时非 null

        LeafNode(String expression) {
            this.expression = expression;
            if ("true".equalsIgnoreCase(expression)) {
                this.constantValue = Boolean.TRUE;
                this.script = null;
            } else if ("false".equalsIgnoreCase(expression)) {
                this.constantValue = Boolean.FALSE;
                this.script = null;
            } else {
                this.constantValue = null;
                // ★ 编译发生在 AST 构建阶段，后续不再重复编译
                this.script = SHARED_SHELL.parse(expression);
            }
        }

        @Override
        boolean evaluate(Map<String, Object> variables) {
            if (constantValue != null) return constantValue;

            Object result = executeScript(script, variables);
            if (result == null) return false;
            if (result instanceof Boolean) return (Boolean) result;
            if (result instanceof List) return !((List<?>) result).isEmpty();
            return true;
        }
    }

    // =========================================================================
    // AST 解析器：字符串 → LogicalNode（只在入口方法调用一次）
    // =========================================================================

    /**
     * 将逻辑表达式字符串解析为 AST。
     * 算符优先级：括号 > NOT(!) > AND(&&) > OR(||)
     * 叶子节点在此处同步编译 Groovy 脚本。
     */
    public static LogicalNode parseLogicalExpression(String expression) {
        expression = expression.trim();

        // --- 1. 剥去被逻辑括号整体包裹的最外层括号 ---
        String stripped = stripOutermostLogicalParens(expression);
        if (!stripped.equals(expression)) {
            return parseLogicalExpression(stripped);
        }

        // --- 2. OR（最低优先级，括号外拆分）---
        List<String> orParts = splitByOperator(expression, "||");
        if (orParts.size() > 1) {
            List<LogicalNode> children = new ArrayList<>();
            for (String part : orParts) children.add(parseLogicalExpression(part.trim()));
            return new OrNode(children);
        }

        // --- 3. AND ---
        List<String> andParts = splitByOperator(expression, "&&");
        if (andParts.size() > 1) {
            List<LogicalNode> children = new ArrayList<>();
            for (String part : andParts) children.add(parseLogicalExpression(part.trim()));
            return new AndNode(children);
        }

        // --- 4. NOT ---
        if (expression.startsWith("!")) {
            return new NotNode(parseLogicalExpression(expression.substring(1).trim()));
        }

        // --- 5. 叶子节点：原子 Gremlin 表达式，同步编译 Script ---
        return new LeafNode(expression);
    }

    /**
     * 若整个字符串被一对"逻辑括号"整体包裹，则剥去最外层。
     * 例如 "(A && B)" → "A && B"，但 "g.V().has('x')" 保持不变。
     */
    private static String stripOutermostLogicalParens(String expression) {
        if (!expression.startsWith("(")) return expression;

        int depth = 0;
        for (int i = 0; i < expression.length(); i++) {
            char c = expression.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') {
                depth--;
                if (depth == 0) {
                    if (i == expression.length() - 1) {
                        // 括号整体包裹，剥去
                        return expression.substring(1, expression.length() - 1).trim();
                    } else {
                        // 括号在中间闭合，不是整体包裹（如 "(...) && (...)"）
                        return expression;
                    }
                }
            }
        }
        return expression;
    }

    // =========================================================================
    // Gremlin 脚本执行（仅替换 Binding，无编译）
    // =========================================================================

    private static Object executeScript(Script script, Map<String, Object> variables) {
        Binding binding = new Binding();
        for (Map.Entry<String, Object> entry : variables.entrySet()) {
            binding.setVariable(entry.getKey(), entry.getValue());
        }
        script.setBinding(binding);

        try {
            Object result = script.run();
            if (result == null) return null;

            if (result instanceof org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal) {
                return ((org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal) result).toList();
            }
            if (result instanceof Iterable) {
                List<Object> list = new ArrayList<>();
                for (Object item : (Iterable<?>) result) list.add(item);
                return list;
            }
            if (result instanceof Boolean) return result;

            return Collections.singletonList(result);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // =========================================================================
    // 运算符拆分工具（括号内不拆）
    // =========================================================================

    private static List<String> splitByOperator(String expression, String operator) {
        List<String> parts = new ArrayList<>();
        int depth = 0, lastSplit = 0;
        int len = expression.length(), opLen = operator.length();

        for (int i = 0; i < len; i++) {
            char c = expression.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (depth == 0 && i <= len - opLen
                    && expression.substring(i, i + opLen).equals(operator)) {
                parts.add(expression.substring(lastSplit, i));
                lastSplit = i + opLen;
                i += opLen - 1;
            }
        }

        if (lastSplit < len) parts.add(expression.substring(lastSplit));
        if (parts.isEmpty()) parts.add(expression);
        return parts;
    }

    // =========================================================================
    // 预计算：接收 AST，n^k 次迭代只做树遍历
    // =========================================================================

    private static void calcSecondOrder(
            List<Vertex> vertices,
            Map<String, Object> variables,
            LogicalNode ast,
            List<Map.Entry<String, String>> conditions,
            int[] coordinates,
            KDimensionalArray results,
            int index) {

        if (index >= conditions.size()) {
            // ★ 直接走 AST 求值，无字符串解析、无脚本编译
            results.set(coordinates, ast.evaluate(variables));
            return;
        }

        String varName = conditions.get(index).getKey();
        int len = vertices.size();

        for (int i = 0; i < len; i++) {
            variables.put(varName, vertices.get(i));
            coordinates[index] = i;
            calcSecondOrder(vertices, variables, ast, conditions, coordinates, results, index + 1);
        }
        variables.remove(varName);
    }

    /**
     * @param ast 由入口方法预先解析好的 LogicalNode AST（叶子已包含编译好的 Script）
     */
    public static void Preconditioning(
            GraphTraversalSource g,
            List<Vertex> vertices,
            LogicalNode ast,
            List<Map.Entry<String, String>> conditions,
            KDimensionalArray results) {

        Map<String, Object> variables = new HashMap<>();
        variables.put("g", g);
        int[] coordinates = new int[conditions.size()];
        calcSecondOrder(vertices, variables, ast, conditions, coordinates, results, 0);
    }

    private static boolean enumerateSecondOrder(
            KDimensionalArray results,
            List<Integer> vertices,
            boolean[] quantifier,
            int[] variables,
            int k,
            int index) {

        if (index >= k) return results.get(variables);

        for (Integer vertex : vertices) {
            variables[index] = vertex;
            boolean r = enumerateSecondOrder(results, vertices, quantifier, variables, k, index + 1);
            if (quantifier[index] && r) return true;
            if (!quantifier[index] && !r) return false;
        }
        return !quantifier[index];
    }

    // =========================================================================
    // 对外公开接口：入口处一次性完成 AST 解析 + Script 编译
    // =========================================================================

    public static boolean secondOrderQuery(
            GraphTraversalSource g,
            String groovyQuery,
            List<Map.Entry<String, String>> conditions) {

        // ★ 调用 Preconditioning 之前：解析 AST，叶子节点同步编译 Script
        LogicalNode ast = parseLogicalExpression(groovyQuery);

        int n = g.V().count().next().intValue();
        int k = conditions.size();
        KDimensionalArray results = new KDimensionalArray(n, k);
        List<Vertex> vertices = g.V().toList();

        Preconditioning(g, vertices, ast, conditions, results);

        List<Integer> vertexIds = new ArrayList<>();
        for (int i = 0; i < n; i++) vertexIds.add(i);

        boolean[] quantifier = new boolean[k];
        for (int i = 0; i < k; i++) quantifier[i] = conditions.get(i).getValue().equals("exist");

        return enumerateSecondOrder(results, vertexIds, quantifier, new int[k], k, 0);
    }

    public static Set<Set<Vertex>> VsetQuery(
            GraphTraversalSource g,
            String groovyQuery,
            String aggregationQuery,
            List<Map.Entry<String, String>> conditions) {

        long time0 = System.currentTimeMillis();

        // ★ 调用 Preconditioning 之前：解析 groovyQuery AST + 编译 aggregationQuery Script
        LogicalNode ast = parseLogicalExpression(groovyQuery);
        Script aggScript = SHARED_SHELL.parse(aggregationQuery);

        int n = g.V().count().next().intValue();
        int k = conditions.size();
        KDimensionalArray results = new KDimensionalArray(n, k);
        List<Vertex> vertices = g.V().toList();

        Preconditioning(g, vertices, ast, conditions, results);

        long time1 = System.currentTimeMillis();

        // aggregationQuery：循环内仅替换 Binding，不重新编译
        boolean[] aggregationTable = new boolean[n + 1];
        for (int i = 0; i <= n; i++) {
            Binding binding = new Binding();
            binding.setVariable("size", i);
            aggScript.setBinding(binding);
            Object result = aggScript.run();
            aggregationTable[i] = (result instanceof Boolean) && (Boolean) result;
        }

        boolean[] quantifierTypes = new boolean[k];
        for (int i = 0; i < k; i++) quantifierTypes[i] = conditions.get(i).getValue().equals("exist");

        Set<Set<Vertex>> validSubsets = new HashSet<>();
        enumerateVset(results, aggregationTable, vertices, new ArrayList<>(),
                quantifierTypes, validSubsets, n, k, 0);

        long time2 = System.currentTimeMillis();
        System.out.println("Preconditioning: " + (time1 - time0) + "ms");
        System.out.println("Enumeration:     " + (time2 - time1) + "ms");

        return validSubsets;
    }

    public static Set<Set<Vertex>> CommunityQuery(
            GraphTraversalSource g,
            String groovyQuery,
            String aggregationQuery,
            List<Map.Entry<String, String>> conditions,
            Set<Set<Vertex>> communities) {

        // ★ 调用 Preconditioning 之前：解析 groovyQuery AST + 编译 aggregationQuery Script
        LogicalNode ast = parseLogicalExpression(groovyQuery);
        Script aggScript = SHARED_SHELL.parse(aggregationQuery);

        int k = conditions.size();
        boolean[] quantifier = new boolean[k];
        for (int i = 0; i < k; i++) quantifier[i] = conditions.get(i).getValue().equals("exist");

        Set<Set<Vertex>> validSubsets = new HashSet<>();

        for (Set<Vertex> community : communities) {
            int n = community.size();

            // aggregationQuery：仅替换 Binding
            Binding binding = new Binding();
            binding.setVariable("size", n);
            aggScript.setBinding(binding);
            Object aggResult = aggScript.run();
            if (!(aggResult instanceof Boolean) || !((Boolean) aggResult)) continue;

            KDimensionalArray results = new KDimensionalArray(n, k);
            List<Vertex> vertices = new ArrayList<>(community);

            // ★ 传入 ast，Preconditioning 内部零编译
            Preconditioning(g, vertices, ast, conditions, results);

            List<Integer> vertexIds = new ArrayList<>();
            for (int i = 0; i < n; i++) vertexIds.add(i);

            if (enumerateSecondOrder(results, vertexIds, quantifier, new int[k], k, 0)) {
                validSubsets.add(community);
            }
        }
        return validSubsets;
    }

    // =========================================================================
    // enumerateVset（不再需要传递 groovyQuery / aggregationQuery 字符串）
    // =========================================================================

    private static void enumerateVset(
            KDimensionalArray results,
            boolean[] aggregationTable,
            List<Vertex> vertices,
            List<Integer> selectedVertices,
            boolean[] quantifier,
            Set<Set<Vertex>> subsets,
            int n, int k, int index) {

        if (index >= n) {
            int size = selectedVertices.size();
            if (!aggregationTable[size]) return;
            if (enumerateSecondOrder(results, selectedVertices, quantifier, new int[k], k, 0)) {
                Set<Vertex> validSubset = new HashSet<>();
                for (int i : selectedVertices) validSubset.add(vertices.get(i));
                subsets.add(validSubset);
            }
            return;
        }

        selectedVertices.add(index);
        enumerateVset(results, aggregationTable, vertices, selectedVertices,
                quantifier, subsets, n, k, index + 1);

        selectedVertices.remove(selectedVertices.size() - 1);
        enumerateVset(results, aggregationTable, vertices, selectedVertices,
                quantifier, subsets, n, k, index + 1);
    }
}
