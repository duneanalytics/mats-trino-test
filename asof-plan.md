# AS OF Join Implementation Plan for Trino

## Executive Summary

AS OF joins match each row from the left table with the most recent row from the right table based on a timestamp. This is critical for financial analytics (trade-quote matching) and time series alignment. We'll deliver a production-ready implementation in 6 weeks by extending Trino's existing join infrastructure.

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

### 1. Leverage Existing Infrastructure

We'll extend `LookupJoinOperator` rather than building from scratch:
- Already handles partitioned execution
- Supports spilling and memory management  
- Integrates with statistics and cost-based optimizer

### 2. Algorithm (Distributed)

**Build Side (quotes):**
1. Hash partition by equality keys (symbol)
2. Sort each partition by timestamp
3. Broadcast sorted partitions to all nodes

**Probe Side (trades):**
1. For each trade, hash to find partition
2. Binary search to find insertion point
3. Return previous entry (most recent quote)

```java
public Page asofJoin(Page probePage, LookupSource lookupSource) {
    BlockBuilder[] builders = new BlockBuilder[outputChannels.size()];
    
    for (int position = 0; position < probePage.getPositionCount(); position++) {
        long joinKey = hashStrategy.hashRow(position, probePage);
        SortedPartition partition = lookupSource.getPartition(joinKey);
        
        if (partition != null) {
            long probeTimestamp = probePage.getLong(timestampChannel, position);
            int matchIndex = partition.findAsOfMatch(probeTimestamp);
            
            if (matchIndex >= 0) {
                partition.appendTo(matchIndex, builders);
            } else {
                appendNulls(builders); // For outer join
            }
        }
    }
    
    return new Page(builders);
}
```

### 3. SQL Syntax

Single syntax aligned with SQL:2016 temporal joins:
```sql
SELECT ... 
FROM left_table
ASOF [LEFT] JOIN right_table
  ON equality_condition AND temporal_condition
```

Where temporal_condition must be one of:
- `left.ts >= right.ts` (most recent)
- `left.ts > right.ts` (strictly before)

## Implementation Steps

### Week 1-2: Parser and Planner
1. Add `ASOF` keyword to SQL parser
2. Create `AsofJoinNode` extending `JoinNode`
3. Add planner rule to recognize pattern:
   ```java
   // In LocalPlanner.java
   if (node instanceof AsofJoinNode) {
       return new AsofJoinOperator(
           buildSource, 
           equalityChannels,
           temporalChannel,
           temporalOperator);
   }
   ```

### Week 3-4: Operator Implementation
1. Extend `LookupSource` with sorted partition support:
   ```java
   interface AsofLookupSource extends LookupSource {
       SortedPartition getPartition(long hashKey);
   }
   ```

2. Implement `AsofJoinOperator`:
   - Reuse `HashBuilderOperator` for partitioning
   - Add sorting phase for build side
   - Binary search for probe matching

3. Handle memory and spilling:
   - Leverage existing `PartitionedLookupSourceFactory`
   - Sort partitions during finalization

### Week 5: Testing
1. **Correctness tests** (TestAsofJoinOperator.java):
   - Basic functionality
   - NULL handling  
   - Empty partitions
   - Time boundary conditions

2. **Distributed tests** (TestDistributedAsofQueries.java):
   - Multi-node execution
   - Skewed data distribution
   - Large scale (1B+ rows)

3. **Benchmarks**:
   - Compare with window function workaround
   - Measure memory usage
   - Profile CPU hotspots

### Week 6: Production Hardening
1. Statistics integration for cost estimation
2. Explain plan visualization
3. Monitoring metrics (rows processed, memory used)
4. Documentation and examples

## Key Design Decisions

### 1. Why extend LookupJoinOperator?
- Reuses battle-tested distributed join infrastructure
- Inherits memory management and spilling
- Minimal changes to query planner

### 2. Why require sorted build side?
- Enables O(log n) probe lookups
- Natural for time-series data (often pre-sorted)
- Can optimize away sort if statistics indicate pre-sorted

### 3. Why binary search over sequential scan?
- Financial data often has millions of quotes per symbol
- Binary search is cache-friendly for repeated probes
- Falls back to sequential for small partitions (<1000 rows)

## Performance Optimizations (Post-MVP)

1. **Metadata tracking**: Skip sort if data arrives pre-sorted
2. **Adaptive algorithm**: Use hash lookup for small time ranges
3. **Vectorization**: SIMD instructions for timestamp comparisons
4. **Pushdown**: Push ASOF joins to connectors that support it

## Success Metrics

1. **Correctness**: Passes all DuckDB AsOf join tests
2. **Performance**: 10x faster than window function workaround
3. **Scale**: Handles 1B trades joining 10B quotes  
4. **Adoption**: Used in 3+ production workloads within Q1

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| OOM on large builds | Reuse existing spilling infrastructure |
| Skewed partitions | Add partition size limits with fallback |
| Complex SQL semantics | Start with simple >= only, expand later |

## Code Structure

```
trino/
├── core/
│   ├── sql/parser/          # Add ASOF keyword
│   ├── sql/planner/         # AsofJoinNode, planner rule
│   └── operator/            # AsofJoinOperator
├── testing/
│   ├── unit/                # TestAsofJoinOperator  
│   └── distributed/         # TestDistributedAsofQueries
└── docs/
    └── sql/join.rst         # Document ASOF JOIN
```

## Next Steps

1. Create GitHub issue with this plan
2. Implement parser changes (PR 1)
3. Add planner support (PR 2) 
4. Implement operator (PR 3)
5. Add tests and docs (PR 4)

Each PR should be <500 lines for easy review. Total implementation: ~2000 lines of production code, ~3000 lines of tests.

---

*This plan focuses on delivering a working ASOF JOIN in 6 weeks. We'll iterate based on user feedback rather than over-engineering upfront.*