# AS OF Join Implementation Plan for Trino

## Executive Summary

AS OF joins match each row from the left table with the most recent row from the right table based on a timestamp. This is critical for financial analytics (trade-quote matching) and time series alignment. We'll deliver a production-ready implementation by extending Trino's existing join infrastructure.

## Core Use Case

**Match trades with their prevailing quotes:**
```sql
SELECT t.*, q.bid, q.ask
FROM trades t
ASOF LEFT JOIN quotes q 
  ON t.symbol = q.symbol 
  AND t.timestamp >= q.timestamp
```

## Technical Approach

### Algorithm (Distributed)

**Build Side (quotes):**
1. Hash partition by equality keys (symbol)
2. Sort each partition by timestamp
3. Broadcast sorted partitions to all nodes

**Probe Side (trades):**
1. For each trade, hash to find partition
2. Binary search to find insertion point
3. Return previous entry (most recent quote)

## Milestone 1: Parser Support

### Deliverables
1. Add `ASOF` keyword to SQL grammar
2. Update `Statement` AST to support ASOF join type
3. Add parser tests

### Code Locations
```
trino-parser/src/main/antlr4/io/trino/grammar/sql/SqlBase.g4
  - Add ASOF to nonReserved
  - Update joinType rule: (INNER | LEFT | RIGHT | FULL | CROSS | ASOF)?

trino-parser/src/main/java/io/trino/sql/tree/Join.java
  - Add ASOF to Type enum

trino-parser/src/test/java/io/trino/sql/parser/TestSqlParser.java
  - Add testAsofJoin() with parsing tests
```

### Example Test
```java
@Test
public void testAsofJoin() {
    assertStatement("SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a AND t1.ts >= t2.ts",
        simpleQuery(selectList(new AllColumns()),
            new Join(Join.Type.ASOF, 
                new Table(QualifiedName.of("t1")),
                new Table(QualifiedName.of("t2")),
                Optional.of(new JoinOn(...)))));
}
```

## Milestone 2: Planner Support

### Deliverables
1. Create `AsofJoinNode` plan node
2. Add analyzer support for ASOF joins
3. Create planner rule to translate to physical operators
4. Add plan optimizer rules

### Code Locations
```
trino-main/src/main/java/io/trino/sql/planner/plan/AsofJoinNode.java
  - New file extending PlanNode
  - Store: left/right sources, equality criteria, temporal criteria

trino-main/src/main/java/io/trino/sql/analyzer/StatementAnalyzer.java
  - In visitJoin(): Handle Join.Type.ASOF
  - Validate temporal condition (must be single >= or >)
  - Extract equality and temporal criteria

trino-main/src/main/java/io/trino/sql/planner/RelationPlanner.java
  - In visitJoin(): Create AsofJoinNode for ASOF joins

trino-main/src/main/java/io/trino/sql/planner/optimizations/PlanOptimizer.java
  - Add AsofJoinPushdown rule (push filters through ASOF joins)
```

### AsofJoinNode Structure
```java
public class AsofJoinNode extends PlanNode {
    private final PlanNode left;
    private final PlanNode right;
    private final List<EquiJoinClause> equalityCriteria;
    private final Symbol leftTimeSymbol;
    private final Symbol rightTimeSymbol;
    private final ComparisonOperator temporalOperator; // >= or >
    private final boolean isOuterJoin;
}
```

## Milestone 3: Operator Implementation

### Deliverables
1. Implement `AsofJoinOperator` and factory
2. Extend `LookupSource` for sorted partitions
3. Add binary search logic for temporal matching
4. Integrate with `LocalExecutionPlanner`

### Code Locations
```
trino-main/src/main/java/io/trino/operator/join/AsofJoinOperator.java
  - New operator extending `Operator`
  - Process probe pages against sorted build partitions

trino-main/src/main/java/io/trino/operator/join/AsofLookupSource.java
  - Interface extending LookupSource
  - Add getPartition(long hashKey) returning SortedPartition

trino-main/src/main/java/io/trino/operator/join/SortedPartition.java
  - Stores sorted rows for a partition
  - Implements findAsOfMatch(long timestamp) using binary search

trino-main/src/main/java/io/trino/sql/planner/LocalExecutionPlanner.java
  - In visitPlan(): Add case for AsofJoinNode
  - Create AsofJoinOperatorFactory
```

### Core Binary Search Implementation
```java
public class SortedPartition {
    private final Page[] pages;
    private final int timestampChannel;
    
    public int findAsOfMatch(long probeTimestamp) {
        int low = 0;
        int high = getTotalPositionCount() - 1;
        int result = -1;
        
        while (low <= high) {
            int mid = (low + high) >>> 1;
            long buildTimestamp = getTimestamp(mid);
            
            if (buildTimestamp <= probeTimestamp) {
                result = mid;  // Potential match
                low = mid + 1; // Look for later timestamp
            } else {
                high = mid - 1;
            }
        }
        
        return result;
    }
}
```

## Milestone 4: Memory Management & Spilling

### Deliverables
1. Integrate with `PartitionedLookupSourceFactory`
2. Add sorting during partition finalization
3. Support spilling large partitions to disk
4. Add memory accounting

### Code Locations
```
trino-main/src/main/java/io/trino/operator/join/AsofJoinBridge.java
  - Coordinates build and probe sides
  - Extends JoinBridge for memory tracking

trino-main/src/main/java/io/trino/operator/join/AsofHashBuilderOperator.java
  - Extends HashBuilderOperator
  - Override finishInput() to sort partitions

trino-main/src/main/java/io/trino/operator/join/SpillableAsofLookupSource.java
  - Implements spilling for large partitions
  - Reuses SpilledLookupSource infrastructure
```

## Milestone 5: Testing

### Deliverables
1. Unit tests for operator
2. Integration tests for distributed execution  
3. Correctness verification against window functions
4. Performance benchmarks

### Code Locations
```
trino-main/src/test/java/io/trino/operator/join/TestAsofJoinOperator.java
  - Test basic functionality, NULLs, empty partitions
  - Test memory limits and spilling

trino-tests/src/test/java/io/trino/tests/TestAsofJoinQueries.java
  - End-to-end SQL tests
  - Compare results with window function equivalent

trino-tests/src/test/java/io/trino/tests/TestAsofJoinDistributed.java
  - Multi-node execution tests
  - Skewed data distribution tests

trino-benchto-benchmarks/src/main/resources/sql/presto/tpcds/asof-join.sql
  - TPC-DS based benchmark queries
```

### Correctness Test Template
```java
@Test
public void testAsofJoinCorrectness() {
    // ASOF JOIN result
    MaterializedResult asofResult = computeActual(
        "SELECT t.*, q.price FROM trades t " +
        "ASOF JOIN quotes q ON t.symbol = q.symbol " +
        "AND t.timestamp >= q.timestamp");
    
    // Window function equivalent
    MaterializedResult windowResult = computeActual(
        "WITH matched AS (" +
        "  SELECT t.*, q.price, " +
        "    ROW_NUMBER() OVER (PARTITION BY t.trade_id " +
        "    ORDER BY q.timestamp DESC) as rn " +
        "  FROM trades t LEFT JOIN quotes q " +
        "  ON t.symbol = q.symbol AND t.timestamp >= q.timestamp" +
        ") SELECT * FROM matched WHERE rn = 1 OR rn IS NULL");
    
    assertEqualsIgnoreOrder(asofResult, windowResult);
}
```

## Milestone 6: Production Readiness

### Deliverables
1. Statistics and cost estimation
2. EXPLAIN plan visualization
3. JMX metrics for monitoring
4. Documentation

### Code Locations
```
trino-main/src/main/java/io/trino/cost/AsofJoinStatsCalculator.java
  - Implement stats calculation for optimizer
  - Consider temporal selectivity

trino-main/src/main/java/io/trino/sql/planner/planprinter/PlanPrinter.java
  - Add ASOF join visualization in EXPLAIN

trino-main/src/main/java/io/trino/operator/join/AsofJoinOperatorStats.java
  - JMX beans for monitoring
  - Track: partitions processed, comparisons, memory used

docs/src/main/sphinx/sql/select.rst
  - Document ASOF JOIN syntax and semantics
  - Add examples for common use cases
```

## Key Design Decisions

### Why extend LookupJoinOperator?
- Reuses battle-tested distributed join infrastructure
- Inherits memory management and spilling
- Minimal changes to query planner

### Why require sorted build side?
- Enables O(log n) probe lookups
- Natural for time-series data (often pre-sorted)
- Can optimize away sort if statistics indicate pre-sorted

### Why binary search over sequential scan?
- Financial data often has millions of quotes per symbol
- Binary search is cache-friendly for repeated probes
- Falls back to sequential for small partitions (<1000 rows)

## Success Criteria

1. **Correctness**: Passes all DuckDB AsOf join tests
2. **Performance**: 10x faster than window function workaround
3. **Scale**: Handles 1B trades joining 10B quotes  
4. **Adoption**: Used in production workloads

## Future Optimizations (Post-MVP)

1. **Metadata tracking**: Skip sort if data arrives pre-sorted
2. **Adaptive algorithm**: Use hash lookup for small time ranges
3. **Vectorization**: SIMD instructions for timestamp comparisons
4. **Pushdown**: Push ASOF joins to connectors that support it

---

*This plan delivers a working ASOF JOIN by building incrementally on Trino's proven infrastructure. Each milestone produces a reviewable, testable component.*