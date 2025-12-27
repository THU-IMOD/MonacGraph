package com.graph.rocks;

import com.graph.rocks.community.CommunityGraph;
import com.graph.rocks.so.SecondOrderTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

/**
 * Demonstration class for second-order logic graph queries and basic Gremlin operations
 * on CommunityGraph implementation
 */
public class Main {

    /**
     * Main execution method demonstrating graph operations and second-order logic queries
     * @param args Command line arguments (unused)
     */
    public static void main(String[] args) {
        // Initialize RocksDB-backed graph instance

        try (Graph graph = CommunityGraph.open("my_database"); graph) {
            SecondOrderTraversalSource g = (SecondOrderTraversalSource) graph.traversal();
            // ==============================
            // Step 1: Initialize sample graph data
            // ==============================
            // Create vertices with explicit IDs and properties
            Vertex alice = graph.addVertex(T.id, 1, T.label, "person", "name", "Alice");
            Vertex bob = graph.addVertex(T.id, 2, T.label, "person", "name", "Bob");
            Vertex charlie = graph.addVertex(T.id, 3, T.label, "person", "name", "Charlie");

            // Create edges between vertices
            alice.addEdge("knows", bob);
            bob.addEdge("knows", charlie);

            // ==============================
            // Step 2: Execute second-order logic queries
            // ==============================
            // Evaluate logical condition: (∀x)(∃y) x has edge to y
            System.out.println("\n=== Execute second-order logic queries ===");
            boolean logicalResult = g.secondOrder()
                    .forall("x")
                    .exist("y")
                    .filter("g.V(x).bothE().otherV().is(y)")
                    .execute();

            System.out.println("Second-order logic query result: " + logicalResult);

            // Find all vertex subsets satisfying the logical condition
            Set<Set<Vertex>> validSubsets = g.Vset()
                    .forall("x")
                    .exist("y")
                    .filter("g.V(x).bothE().otherV().is(y)")
                    .execute();

            // Convert vertex subsets to readable format (name properties)
            Set<Set<Object>> readableSubsets = new HashSet<>();
            for (Set<Vertex> vertexSet : validSubsets) {
                Set<Object> nameSet = new HashSet<>();
                for (Vertex vertex : vertexSet) {
                    nameSet.add(vertex.value("name"));
                }
                readableSubsets.add(nameSet);
            }

            System.out.println("Valid vertex subsets (by name): " + readableSubsets);

            // Clear sample data for fresh operations
            g.V().drop().iterate();

            // ==============================
            // Step 3: Basic Gremlin operations demonstration
            // ==============================
            // Create user and product vertices with properties
            Vertex userAlice = g.addV("user")
                    .property(T.id, 1)
                    .property("name", "Alice")
                    .property("age", 28)
                    .next();

            Vertex userBob = g.addV("user")
                    .property(T.id, 2)
                    .property("name", "Bob")
                    .property("age", 30)
                    .next();

            Vertex productPhone = g.addV("product")
                    .property(T.id, 3)
                    .property("name", "手机")
                    .property("price", 2999)
                    .next();

            System.out.println("\n=== Vertex Creation Complete ===");
            System.out.println("Alice vertex: " + userAlice);
            System.out.println("Bob vertex: " + userBob);
            System.out.println("Phone vertex: " + productPhone);

            // Create edges with properties
            g.V(userAlice).addE("friend").to(userBob).property("since", 2020).next();
            g.V(userAlice).addE("buy").to(productPhone).property("time", 2025).property("amount", 1).next();

            System.out.println("\n=== Edge Creation Complete ===");

            // ==============================
            // Step 4: Execute basic Gremlin queries
            // ==============================
            System.out.println("\n=== Executing Gremlin Queries ===");

            // Query 1: Get name and age of all users
            System.out.println("\n1. All users (name, age):");
            g.V().hasLabel("user").valueMap("name", "age").forEachRemaining(System.out::println);

            // Query 2: Get names of Alice's friends
            System.out.println("\n2. Alice's friends (name):");
            g.V().has("name", "Alice").out("friend").values("name").forEachRemaining(System.out::println);

            // Query 3: Get products Alice bought (name, price)
            System.out.println("\n3. Products bought by Alice:");
            g.V().has("name", "Alice").out("buy").valueMap("name", "price").forEachRemaining(System.out::println);

            // Query 4: Count total users
            long userCount = g.V().hasLabel("user").count().next();
            System.out.println("\n4. Total users: " + userCount);

            // ==============================
            // Step 5: Update vertex properties
            // ==============================
            System.out.println("\n=== Updating Vertex Properties ===");
            g.V().has("name", "Alice").property("age", 29).next();
            System.out.println("Alice's updated age: " + g.V().has("name", "Alice").values("age").next());

            // ==============================
            // Step 6: Delete graph elements
            // ==============================
            System.out.println("\n=== Deleting Graph Elements ===");

            // Count and remove Alice->Bob friend edge
            long friendEdgeCount = g.V().has("name", "Alice")
                    .outE("friend")
                    .where(__.inV().has("name", "Bob"))
                    .count()
                    .next();

            System.out.println("Friend edge count (Alice->Bob) before deletion: " + friendEdgeCount);

            if (friendEdgeCount > 0) {
                g.V().has("name", "Alice")
                        .outE("friend")
                        .where(__.inV().has("name", "Bob"))
                        .drop()
                        .iterate();
                System.out.println("Successfully deleted Alice->Bob friend edge");
            } else {
                System.out.println("No friend edge found between Alice and Bob");
            }

            // Verify edge deletion
            long remainingFriends = g.V().has("name", "Alice").out("friend").count().next();
            System.out.println("Alice's remaining friends count: " + remainingFriends);

        } catch (Exception e) {
            // Handle and log any exceptions during graph operations
            e.printStackTrace(System.err);
        } finally {
            // ==============================
            // Cleanup: Close graph connection automatically
            // ==============================
            System.out.println("\n=== Graph Database Closed ===");
        }
    }
}