# AsOf Join Implementation Plan

## Table of Contents
1. [Overview](#overview)
2. [Conceptual Foundation](#conceptual-foundation)
3. [Architecture Design](#architecture-design)
4. [Core Algorithm](#core-algorithm)
5. [Implementation Details](#implementation-details)
6. [Performance Optimizations](#performance-optimizations)
7. [SQL Syntax and API](#sql-syntax-and-api)
8. [Use Cases and Examples](#use-cases-and-examples)
9. [Testing Strategy](#testing-strategy)
10. [Future Enhancements](#future-enhancements)

## Overview

AsOf joins are specialized temporal joins that match rows based on the nearest preceding (or following) value in time. Unlike regular joins that require exact matches, AsOf joins find the most recent value "as of" a given time, making them essential for time-series analysis, financial data processing, and IoT applications.

### Key Characteristics
- Returns at most one match from the right table for each left table row
- Handles time-series data with misaligned timestamps
- Supports both equality and inequality conditions
- Optimized for temporal lookups

## Conceptual Foundation

### Event Tables vs State Tables
- **Event Tables**: Store point-in-time changes (e.g., price updates)
- **State Tables**: Store ranges with start/end times
- AsOf joins allow treating event tables as state tables without materialization

### Inequality Types and Intervals
| Inequality | Interval | Interpretation |
|------------|----------|----------------|
| >= | [Tn, Tn+1) | Time marks interval start, included |
| > | (Tn, Tn+1] | Time marks interval start, excluded |
| <= | (Tn-1, Tn] | Time marks interval end, included |
| < | [Tn-1, Tn) | Time marks interval end, excluded |

## Architecture Design

### Component Overview
```
┌─────────────────────────────────────────────────────────────┐
│                    Query Planner                             │
│  - Detects AsOf join patterns                               │
│  - Chooses execution strategy                               │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                 AsOf Join Operator                          │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐   │
│  │   Sink      │  │   Operator   │  │     Source      │   │
│  │  (Build)    │  │  (Process)   │  │    (Output)     │   │
│  └─────────────┘  └──────────────┘  └─────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Pipeline Phases

1. **Sink Phase**: Build side processing
   - Hash partition on equality columns
   - Sort within partitions on inequality column
   - Create temporary state table structure

2. **Operator Phase**: Probe side processing
   - Filter NULL predicate values
   - Hash partition matching build side
   - Sort within partitions
   - Cache for merge phase

3. **Source Phase**: Join execution
   - Match hash partitions
   - Merge join with early stopping
   - Output matched rows

## Core Algorithm

### Standard AsOf Join Algorithm

```python
def asof_join(probe_table, build_table, equality_cols, time_col, inequality):
    # Phase 1: Partition and sort build side
    build_partitions = hash_partition(build_table, equality_cols)
    for partition in build_partitions:
        sort(partition, time_col)
    
    # Phase 2: Partition and sort probe side
    probe_partitions = hash_partition(probe_table, equality_cols)
    for partition in probe_partitions:
        sort(partition, time_col)
    
    # Phase 3: Merge join with AsOf semantics
    results = []
    for eq_key in partition_keys:
        build_part = build_partitions[eq_key]
        probe_part = probe_partitions[eq_key]
        
        # Merge with early stopping
        build_idx = 0
        for probe_row in probe_part:
            # Find last matching build row
            while (build_idx < len(build_part) and 
                   satisfies_inequality(build_part[build_idx][time_col], 
                                      probe_row[time_col], 
                                      inequality)):
                build_idx += 1
            
            if build_idx > 0:
                # Match found - use previous row
                results.append(join_rows(probe_row, build_part[build_idx - 1]))
            
    return results
```

### Loop Join Optimization (for small probe tables)

```python
def asof_loop_join(probe_table, build_table, threshold=64):
    if len(probe_table) > threshold:
        return standard_asof_join(probe_table, build_table)
    
    # Add row numbers for grouping
    probe_with_pk = add_row_numbers(probe_table)
    
    # Nested loop join
    matches = []
    for probe_row in probe_with_pk:
        for build_row in build_table:
            if satisfies_all_conditions(probe_row, build_row):
                matches.append((probe_row.pk, probe_row, build_row))
    
    # Group by probe PK and select most recent
    results = []
    for pk, group in group_by(matches, 'pk'):
        best_match = arg_max(group, build_time_column)
        results.append(best_match)
    
    return results
```

## Implementation Details

### Data Structures

#### Partition Hash Table
```cpp
struct PartitionHashTable {
    std::unordered_map<HashKey, std::vector<Row>> partitions;
    std::vector<ColumnType> equality_columns;
    
    HashKey compute_hash(const Row& row);
    void insert(const Row& row);
};
```

#### Sorted Partition
```cpp
struct SortedPartition {
    std::vector<Row> rows;
    size_t time_column_idx;
    ComparisonType comparison;
    
    void sort();
    size_t find_asof_match(const Value& probe_time);
};
```

### Memory Management
- Use chunked processing for large datasets
- Implement spilling to disk when memory threshold exceeded
- Reuse buffers across partitions

### NULL Handling
- NULL in equality columns: no match
- NULL in inequality column: no match
- Outer AsOf joins return NULL values for non-matching rows

## Performance Optimizations

### 1. Early Termination
- Stop searching once a match is found (at most one match per probe row)
- Leverage sorted order to use binary search where applicable

### 2. Partition Pruning
- Skip empty partitions early
- Use bloom filters for existence checks

### 3. Adaptive Algorithm Selection
```python
def choose_algorithm(probe_size, build_size, available_memory):
    if probe_size <= LOOP_JOIN_THRESHOLD:  # Default: 64
        return "loop_join"
    elif build_size * ROW_SIZE <= available_memory:
        return "standard_asof"
    else:
        return "external_asof"  # With spilling
```

### 4. Metadata Tracking (Future)
- Track if data is pre-sorted to skip sorting phase
- Maintain partition statistics for better planning

### 5. Parallelization
- Parallel partition processing
- Parallel sorting within partitions
- Thread-safe result collection

## SQL Syntax and API

### SQL Syntax Options

#### Option 1: ON Clause
```sql
SELECT *
FROM probe_table p
ASOF JOIN build_table b
  ON p.symbol = b.symbol
 AND p.exchange = b.exchange
 AND p.timestamp >= b.timestamp;
```

#### Option 2: USING Clause
```sql
SELECT *
FROM probe_table p
ASOF JOIN build_table b
  USING (symbol, exchange, timestamp);
-- Last column uses >= by default
```

#### Option 3: WITH Direction
```sql
SELECT *
FROM probe_table p
ASOF JOIN build_table b
  ON p.symbol = b.symbol
  WITH p.timestamp >= b.timestamp;
```

### Outer AsOf Join
```sql
SELECT *
FROM probe_table p
ASOF LEFT JOIN build_table b
  USING (symbol, timestamp);
```

### API Design
```python
class AsOfJoin:
    def __init__(self, 
                 inequality_type='>=',
                 loop_join_threshold=64):
        self.inequality_type = inequality_type
        self.threshold = loop_join_threshold
    
    def execute(self, 
                probe_table, 
                build_table,
                equality_columns,
                time_column,
                outer=False):
        # Implementation
        pass
```

## Use Cases and Examples

### 1. Financial Data: Trade and Quote Matching
```sql
-- Find prevailing quote for each trade
SELECT 
    t.symbol,
    t.trade_time,
    t.price as trade_price,
    q.bid,
    q.ask,
    (q.bid + q.ask) / 2 as mid_price
FROM trades t
ASOF JOIN quotes q
    USING (symbol, exchange, trade_time);
```

### 2. IoT Sensor Data Alignment
```sql
-- Align different sensor readings
SELECT 
    s1.timestamp,
    s1.temperature,
    s2.humidity,
    s3.pressure
FROM sensor_1min s1
ASOF JOIN sensor_5min s2
    ON s1.timestamp >= s2.timestamp
ASOF JOIN sensor_10min s3
    ON s1.timestamp >= s3.timestamp;
```

### 3. Slowly Changing Dimensions
```sql
-- Get product info as of order date
SELECT 
    o.order_id,
    o.order_date,
    o.product_id,
    p.product_name,
    p.category,
    p.price as price_at_order_time
FROM orders o
ASOF JOIN product_history p
    ON o.product_id = p.product_id
   AND o.order_date >= p.effective_date;
```

## Testing Strategy

### Unit Tests
1. **Basic Functionality**
   - Single equality column
   - Multiple equality columns
   - Different inequality types
   - NULL handling

2. **Edge Cases**
   - Empty tables
   - Single row tables
   - All NULLs
   - No matches
   - Duplicate timestamps

3. **Data Types**
   - Timestamp/DateTime
   - Date
   - Integer sequences
   - Floating point

### Performance Tests
1. **Scalability**
   - Vary probe table size (1K to 1B rows)
   - Vary build table size
   - Different partition counts

2. **Memory Usage**
   - Monitor memory consumption
   - Test spilling behavior
   - Memory leak detection

3. **Optimization Verification**
   - Loop join vs standard algorithm
   - Pre-sorted data performance
   - Parallel execution speedup

### Correctness Tests
1. **Compare with SQL Alternative**
```sql
-- AsOf join result
WITH asof_result AS (
    SELECT * FROM t1 ASOF JOIN t2 USING (key, time)
)
-- SQL alternative
, sql_result AS (
    SELECT t1.*, 
           FIRST_VALUE(t2.value) OVER (
               PARTITION BY t1.key 
               ORDER BY t2.time DESC
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) as value
    FROM t1
    LEFT JOIN t2 
        ON t1.key = t2.key 
       AND t1.time >= t2.time
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY t1.key, t1.time 
        ORDER BY t2.time DESC
    ) = 1
)
-- Results should match
SELECT * FROM asof_result
EXCEPT
SELECT * FROM sql_result;
```

## Future Enhancements

### 1. Metadata and Statistics
- Track sorted columns across operators
- Maintain partition statistics
- Cost-based algorithm selection

### 2. Additional Join Types
- ASOF INNER/LEFT/RIGHT/FULL variants
- Bi-directional AsOf (nearest in either direction)
- AsOf with windows (match within time range)

### 3. Optimizations
- Vectorized comparison operations
- SIMD instructions for sorting
- GPU acceleration for large joins
- Compressed partition storage

### 4. Extended Syntax
```sql
-- Range-limited AsOf
SELECT *
FROM trades t
ASOF JOIN quotes q
    USING (symbol, time)
    WITHIN INTERVAL '5 minutes';

-- Nearest neighbor (bi-directional)
SELECT *
FROM events e1
ASOF NEAREST JOIN events e2
    ON e1.id != e2.id
    USING (location, timestamp);
```

### 5. Monitoring and Diagnostics
- Execution statistics (partitions, comparisons, memory)
- Query plan visualization
- Performance profiling hooks

## Implementation Timeline

### Phase 1: Core Implementation (2-3 months)
- Basic AsOf join operator
- Standard algorithm
- SQL parser integration
- Basic testing

### Phase 2: Optimizations (1-2 months)
- Loop join for small tables
- Parallel execution
- Memory management
- Performance testing

### Phase 3: Extended Features (2-3 months)
- Additional join types
- Extended syntax options
- Metadata tracking
- Production hardening

### Phase 4: Advanced Optimizations (Ongoing)
- Vectorization
- GPU support
- Advanced statistics
- Auto-tuning

## Conclusion

Implementing an AsOf join similar to DuckDB's requires careful attention to:
1. Efficient algorithms that leverage sorting and early termination
2. Flexible syntax that supports various use cases
3. Performance optimizations for different data sizes
4. Robust handling of edge cases and NULL values
5. Integration with existing query planning and execution framework

The implementation should start with the core algorithm and progressively add optimizations based on real-world usage patterns and performance requirements.