# BDM-lab1

# 🧠 Academic Graph Database with Neo4j

This project models and populates a synthetic academic citation network using **Neo4j**. We simulate relationships between authors, papers, conferences, journals, and organizations, then analyze the resulting graph using Cypher queries and graph algorithms.

## 📘 Overview
- Built a graph schema for academic publishing (papers, authors, keywords, reviews, etc.)
- Populated the database with **synthetic data** via Python and Faker
- Queried the graph to compute metrics like h-index, impact factor, and top papers
- Applied **PageRank** to rank paper influence beyond simple citation counts

## 🔧 Graph Modeling

### Nodes
- `Author`, `Paper`, `Review`, `Conference`, `Edition`, `Journal`, `Volume`, `Org`, `Keyword`

### Relationships
- `AUTHORED`, `APPEARED_IN`, `HAS_REVIEW`, `CITES`, `AFFILIATED_TO`, `HAS_KEYWORD`, `VOTE`, etc.
- Role metadata included on edges (e.g., lead author vs. coauthor)

## 🛠 Data Generation & Loading
- Implemented in Python with two main classes:
  - `DataGenerator`: creates all entities and relationships
  - `Loader`: connects to a Neo4j instance and loads batched Cypher queries
- All nodes identified by UUIDs for uniqueness
- Organizations are linked to authors probabilistically to simulate real-world data sparsity

## 📈 Graph Queries
- Top-3 most cited papers per conference
- Community detection: prolific authors per conference (≥4 editions)
- Journal impact factors based on 2-year citation windows
- Author h-index using Cypher

## 📊 Graph Algorithms
- Applied **PageRank** to evaluate paper influence
- Demonstrated that PageRank captures citation quality, not just count
- Rank examples show divergence between raw citations and PageRank score

## 👥 Authors
DSDM Lab 1 — BSE 2024-2025  
Denis Shadrin, Noemi Lucchi
