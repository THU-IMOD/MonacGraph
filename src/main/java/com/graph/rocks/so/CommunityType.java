package com.graph.rocks.so;

/**
 * Enumeration representing different types of community detection algorithms.
 */
public enum CommunityType {
    /**
     * Represents a general community structure (default community detection).
     */
    COMMUNITY,

    /**
     * Represents Weakly Connected Components in an undirected graph.
     */
    WCC,

    /**
     * Represents Strongly Connected Components in a directed graph.
     */
    SCC;
}