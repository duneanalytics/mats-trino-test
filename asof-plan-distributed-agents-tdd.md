# ASOF Join Implementation Plan for Distributed Coding Agents (TDD Approach)

## Overview

This document contains self-contained implementation tasks for adding ASOF JOIN support to Trino. Each task is designed to be completed independently by a coding agent following Test-Driven Development (TDD) principles.

**IMPORTANT TDD WORKFLOW FOR ALL TASKS:**
1. **Write tests FIRST** - Create failing tests that define the expected behavior
2. **Run tests** - Verify tests fail with meaningful error messages
3. **Implement minimal code** - Write just enough code to make tests pass
4. **Refactor** - Improve code quality while keeping tests green
5. **Repeat** - Continue this cycle for each feature

---

## Agent Task 1: SQL Parser Support for ASOF JOIN

### Context
ASOF joins are temporal joins that match each row from the left table with the most recent row from the right table based on a timestamp. This is critical for financial analytics (e.g., matching trades with prevailing quotes) and time series alignment.

Example SQL:
```sql
SELECT t.*, q.bid, q.ask
FROM trades t
ASOF LEFT JOIN quotes q 
  ON t.symbol = q.symbol 
  AND t.timestamp >= q.timestamp
```

### Your Task
Add parser support for the ASOF keyword and join type in Trino's SQL grammar using Test-Driven Development.

### Test-First Development Steps

#### Step 1: Write Failing Parser Tests
**Start here** - Create tests in `trino-parser/src/test/java/io/trino/sql/parser/TestSqlParser.java`:

```java
@Test
public void testAsofJoin() {
    // This test should FAIL initially - ASOF is not recognized
    assertStatement("SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a AND t1.ts >= t2.ts",
        simpleQuery(selectList(new AllColumns()),
            new Join(Join.Type.ASOF, 
                new Table(QualifiedName.of("t1")),
                new Table(QualifiedName.of("t2")),
                Optional.of(new JoinOn(
                    new LogicalBinaryExpression(AND,
                        new ComparisonExpression(EQUAL, 
                            new DereferenceExpression(new Identifier("t1"), new Identifier("a")),
                            new DereferenceExpression(new Identifier("t2"), new Identifier("a"))),
                        new ComparisonExpression(GREATER_THAN_OR_EQUAL,
                            new DereferenceExpression(new Identifier("t1"), new Identifier("ts")),
                            new DereferenceExpression(new Identifier("t2"), new Identifier("ts")))))))));
}

@Test
public void testAsofLeftJoin() {
    // This should also FAIL initially
    assertStatement("SELECT * FROM trades ASOF LEFT JOIN quotes ON trades.symbol = quotes.symbol AND trades.time >= quotes.time",
        /* expected AST structure */);
}

@Test
public void testAsofJoinInvalidSyntax() {
    // Test that ASOF FULL JOIN is rejected
    assertFails("SELECT * FROM t1 ASOF FULL JOIN t2 ON t1.a = t2.a")
        .hasMessage("ASOF joins do not support FULL join type");
}
```

**Run tests to confirm they fail:**
```bash
./mvnw test -pl :trino-parser -Dtest=TestSqlParser#testAsofJoin
# Expected: ParseException - no viable alternative at input 'ASOF'
```

#### Step 2: Implement Grammar Changes
Now implement the minimal changes to make tests pass:

1. **Update SQL Grammar** (`trino-parser/src/main/antlr4/io/trino/grammar/sql/SqlBase.g4`):
   ```antlr
   // Add to nonReserved rule
   nonReserved
       : ...
       | ASOF
       ...
       ;
   
   // Update joinType rule
   joinType
       : INNER?
       | CROSS
       | LEFT OUTER?
       | RIGHT OUTER?
       | FULL OUTER?
       | ASOF LEFT?
       | ASOF RIGHT?
       | ASOF  // inner ASOF join
       ;
   ```

2. **Regenerate parser** and run tests:
   ```bash
   ./mvnw antlr4:antlr4 -pl :trino-parser
   ./mvnw test -pl :trino-parser -Dtest=TestSqlParser#testAsofJoin
   # Expected: Now fails on Join.Type.ASOF not existing
   ```

#### Step 3: Add Join Type Test
Create a test for the Join AST node:

```java
// In trino-parser/src/test/java/io/trino/sql/tree/TestJoin.java (create if needed)
@Test
public void testAsofJoinType() {
    Join asofJoin = new Join(Join.Type.ASOF, 
        new Table(QualifiedName.of("t1")),
        new Table(QualifiedName.of("t2")),
        Optional.empty());
    
    assertEquals(asofJoin.getType(), Join.Type.ASOF);
    assertEquals(asofJoin.toString(), "t1 ASOF JOIN t2");
}
```

#### Step 4: Implement Join Type
Update `trino-parser/src/main/java/io/trino/sql/tree/Join.java`:
```java
public enum Type {
    CROSS,
    INNER,
    LEFT,
    RIGHT,
    FULL,
    ASOF;  // Add this
    
    @Override
    public String toString() {
        return switch (this) {
            case CROSS -> "CROSS";
            case INNER -> "INNER";
            case LEFT -> "LEFT";
            case RIGHT -> "RIGHT";
            case FULL -> "FULL";
            case ASOF -> "ASOF";  // Add this
        };
    }
}
```

#### Step 5: Add Formatter Test
```java
// In TestSqlFormatter.java
@Test
public void testFormatAsofJoin() {
    String sql = "SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a";
    assertFormattedSql(getParserOptions(), sql, sql);
}
```

#### Step 6: Update Formatter
Only after the test fails, implement in `SqlFormatter.java`:
```java
@Override
protected Void visitJoin(Join node, Context context) {
    // ... existing code ...
    switch (node.getType()) {
        // ... existing cases ...
        case ASOF -> builder.append("ASOF JOIN");
    }
    // ...
}
```

### Testing Verification Points
After each implementation step, run:
```bash
# Run all parser tests
./mvnw test -pl :trino-parser

# Run specific test
./mvnw test -pl :trino-parser -Dtest=TestSqlParser#testAsofJoin
```

### Success Criteria
- All new tests pass
- No existing tests are broken
- Parser correctly recognizes ASOF keyword
- AST correctly represents ASOF join type
- Round-trip through parser and formatter preserves syntax

---

## Agent Task 2: Query Analyzer Support for ASOF JOIN (TDD)

### Context
ASOF joins require special validation - they must have exactly one temporal condition (>= or >) and zero or more equality conditions. The analyzer must extract these conditions separately for the planner.

### Dependencies
- Agent Task 1 (Parser Support) must be complete

### Your Task
Add analyzer support to validate ASOF join syntax and extract equality/temporal criteria using Test-Driven Development.

### Test-First Development Steps

#### Step 1: Write Analyzer Test Infrastructure
Create `trino-main/src/test/java/io/trino/sql/analyzer/TestAsofJoinAnalysis.java`:

```java
public class TestAsofJoinAnalysis extends BaseAnalyzerTest {
    
    @Test
    public void testValidAsofJoin() {
        // This should FAIL initially - no ASOF join support in analyzer
        analyze("SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a AND t1.ts >= t2.ts");
        // Should succeed when implemented
    }
    
    @Test
    public void testAsofJoinMissingTemporalCondition() {
        // Should fail with specific error
        assertFails("SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a")
            .hasErrorCode(MISSING_TEMPORAL_CONDITION)
            .hasMessage("ASOF join requires exactly one temporal condition (>= or >)");
    }
    
    @Test
    public void testAsofJoinMultipleTemporalConditions() {
        assertFails("SELECT * FROM t1 ASOF JOIN t2 ON t1.ts >= t2.ts AND t1.ts2 >= t2.ts2")
            .hasErrorCode(MULTIPLE_TEMPORAL_CONDITIONS)
            .hasMessage("ASOF join requires exactly one temporal condition");
    }
    
    @Test
    public void testAsofJoinInvalidTemporalOperator() {
        assertFails("SELECT * FROM t1 ASOF JOIN t2 ON t1.ts <= t2.ts")
            .hasErrorCode(INVALID_TEMPORAL_OPERATOR)
            .hasMessage("ASOF join temporal condition must use >= or > operator");
    }
    
    @Test
    public void testAsofJoinWithUsing() {
        assertFails("SELECT * FROM t1 ASOF JOIN t2 USING (symbol)")
            .hasErrorCode(INVALID_ASOF_JOIN)
            .hasMessage("ASOF join does not support USING clause");
    }
}
```

**Run tests to verify they fail:**
```bash
./mvnw test -pl :trino-main -Dtest=TestAsofJoinAnalysis
# Expected: Tests fail because ASOF join is not handled in analyzer
```

#### Step 2: Create AsofJoinCriteria Test
```java
// In TestAsofJoinAnalyzer.java (new file)
@Test
public void testAsofJoinCriteriaExtraction() {
    Expression joinCondition = new LogicalBinaryExpression(AND,
        new ComparisonExpression(EQUAL,
            new Identifier("t1.symbol"),
            new Identifier("t2.symbol")),
        new ComparisonExpression(GREATER_THAN_OR_EQUAL,
            new Identifier("t1.ts"),
            new Identifier("t2.ts")));
    
    AsofJoinCriteria criteria = AsofJoinAnalyzer.analyzeAsofJoin(joinCondition, analysis);
    
    assertEquals(criteria.getEqualityConditions().size(), 1);
    assertEquals(criteria.getTemporalOperator(), GREATER_THAN_OR_EQUAL);
    assertNotNull(criteria.getLeftTemporal());
    assertNotNull(criteria.getRightTemporal());
}
```

#### Step 3: Implement AsofJoinAnalyzer
Only after tests fail, create `AsofJoinAnalyzer.java`:

```java
public class AsofJoinAnalyzer {
    // Start with minimal implementation
    public static AsofJoinCriteria analyzeAsofJoin(Expression criteria, Analysis analysis) {
        // Implementation to make tests pass
        return new AsofJoinCriteria(/* ... */);
    }
    
    public static class AsofJoinCriteria {
        // Only add fields needed to make tests pass
    }
}
```

#### Step 4: Integration Test
```java
@Test
public void testAsofJoinEndToEnd() {
    // Full analyzer test
    Analysis analysis = analyzeStatement("SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a AND t1.ts >= t2.ts");
    
    // Verify ASOF criteria were extracted
    Join joinNode = /* extract join node from analyzed statement */;
    AsofJoinCriteria criteria = analysis.getAsofJoinCriteria(joinNode);
    assertNotNull(criteria);
}
```

### Testing Verification Points
- Each test should fail initially with clear error messages
- Implement only enough code to make the current test pass
- Run all tests after each implementation step
- Refactor only when all tests are green

### Success Criteria
- All analyzer tests pass
- ASOF joins are validated for correct temporal conditions
- Invalid syntax produces specific, helpful error messages
- Integration with existing analyzer infrastructure works correctly

---

## Agent Task 3: Plan Node for ASOF JOIN (TDD)

### Context
The planner needs a dedicated plan node to represent ASOF joins in the query plan. This node will store the logical representation before being translated to physical operators.

### Dependencies
- Agent Task 2 (Analyzer Support) must be complete

### Your Task
Create the AsofJoinNode plan node and integrate it with the query planner using Test-Driven Development.

### Test-First Development Steps

#### Step 1: Write Plan Node Tests
Create `trino-main/src/test/java/io/trino/sql/planner/plan/TestAsofJoinNode.java`:

```java
public class TestAsofJoinNode {
    
    @Test
    public void testAsofJoinNodeCreation() {
        // Define expected structure
        PlanNodeId id = new PlanNodeId("asof-1");
        Symbol leftTime = new Symbol("lt");
        Symbol rightTime = new Symbol("rt");
        
        AsofJoinNode node = new AsofJoinNode(
            id,
            createTableScan("left", "a", "lt"),
            createTableScan("right", "b", "rt"),
            ImmutableList.of(new EquiJoinClause(new Symbol("a"), new Symbol("b"))),
            leftTime,
            rightTime,
            GREATER_THAN_OR_EQUAL,
            false, // not left join
            false  // not right join
        );
        
        assertEquals(node.getId(), id);
        assertEquals(node.getLeftTimeSymbol(), leftTime);
        assertEquals(node.getRightTimeSymbol(), rightTime);
        assertEquals(node.getTemporalOperator(), GREATER_THAN_OR_EQUAL);
    }
    
    @Test
    public void testOutputSymbols() {
        AsofJoinNode innerJoin = createAsofJoinNode(false, false);
        assertEquals(
            innerJoin.getOutputSymbols(),
            ImmutableList.of("left.a", "left.lt", "right.b", "right.rt"));
        
        AsofJoinNode leftJoin = createAsofJoinNode(true, false);
        // Left join should include all symbols
        assertTrue(leftJoin.getOutputSymbols().containsAll(
            ImmutableList.of("left.a", "left.lt", "right.b", "right.rt")));
    }
    
    @Test
    public void testPlanVisitor() {
        AsofJoinNode node = createAsofJoinNode(false, false);
        TestPlanVisitor visitor = new TestPlanVisitor();
        
        node.accept(visitor, null);
        
        assertTrue(visitor.visitedAsofJoin);
    }
}
```

**Run to verify compilation errors:**
```bash
./mvnw compile test-compile -pl :trino-main
# Expected: AsofJoinNode class does not exist
```

#### Step 2: Create Minimal AsofJoinNode
Create just enough to compile:

```java
// AsofJoinNode.java
public class AsofJoinNode extends PlanNode {
    // Add only fields needed for tests
    
    @Override
    public List<Symbol> getOutputSymbols() {
        // Minimal implementation
        return ImmutableList.of();
    }
    
    @Override
    public List<PlanNode> getSources() {
        return ImmutableList.of(left, right);
    }
    
    @Override
    public PlanNode accept(PlanVisitor<?, ?> visitor, Object context) {
        return visitor.visitAsofJoin(this, context);
    }
}
```

#### Step 3: Write Planner Integration Test
```java
// In TestAsofJoinPlanning.java
@Test
public void testAsofJoinPlanning() {
    // This should fail until RelationPlanner handles ASOF joins
    Plan plan = plan("SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a AND t1.ts >= t2.ts");
    
    // Verify plan contains AsofJoinNode
    PlanNode root = plan.getRoot();
    List<AsofJoinNode> asofJoins = PlanNodeSearcher.searchFrom(root)
        .whereIsInstanceOfAny(AsofJoinNode.class)
        .findAll();
    
    assertEquals(asofJoins.size(), 1);
    
    AsofJoinNode asofJoin = asofJoins.get(0);
    assertEquals(asofJoin.getEqualityCriteria().size(), 1);
    assertEquals(asofJoin.getTemporalOperator(), GREATER_THAN_OR_EQUAL);
}
```

#### Step 4: Test PlanPrinter
```java
@Test
public void testAsofJoinPlanPrinting() {
    String plan = getPlan("SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a AND t1.ts >= t2.ts");
    
    assertThat(plan).contains("AsofJoin[>=]");
    assertThat(plan).contains("Equality: [a = b]");
    assertThat(plan).contains("Temporal: lt >= rt");
}
```

### Testing Verification Points
- Write test for each public method before implementing
- Verify tests fail with expected errors
- Implement minimal code to pass tests
- Refactor for clarity while keeping tests green

### Success Criteria
- AsofJoinNode correctly models ASOF join semantics
- Plan can be created from analyzed ASOF join queries
- EXPLAIN output clearly shows ASOF join details
- All tests pass without breaking existing functionality

---

## Agent Task 4: ASOF Join Operator Implementation (TDD)

### Context
The operator is responsible for the actual execution of ASOF joins. It needs to efficiently match each probe row with the most recent build row using binary search on sorted partitions.

### Dependencies
- Agent Task 3 (Plan Node) must be complete

### Your Task
Implement the core ASOF join operator that performs the temporal matching logic using Test-Driven Development.

### Test-First Development Steps

#### Step 1: Write Binary Search Tests
Start with the core algorithm - create `TestSortedPartition.java`:

```java
public class TestSortedPartition {
    
    @Test
    public void testFindAsOfMatchSinglePage() {
        // Create test data: timestamps [100, 200, 300]
        Page page = new Page(
            createLongsBlock(100, 200, 300),
            createStringsBlock("A", "B", "C"));
        
        SortedPartition partition = new SortedPartition(
            ImmutableList.of(page), 0); // timestamp channel = 0
        
        // Test exact match
        assertEquals(partition.findAsOfMatch(200, GREATER_THAN_OR_EQUAL), 1);
        
        // Test between values
        assertEquals(partition.findAsOfMatch(250, GREATER_THAN_OR_EQUAL), 1);
        
        // Test before all values
        assertEquals(partition.findAsOfMatch(50, GREATER_THAN_OR_EQUAL), -1);
        
        // Test after all values
        assertEquals(partition.findAsOfMatch(350, GREATER_THAN_OR_EQUAL), 2);
    }
    
    @Test
    public void testFindAsOfMatchMultiplePages() {
        // Test with data split across pages
        Page page1 = new Page(createLongsBlock(100, 200));
        Page page2 = new Page(createLongsBlock(300, 400));
        
        SortedPartition partition = new SortedPartition(
            ImmutableList.of(page1, page2), 0);
        
        // Test cross-page boundary
        assertEquals(partition.findAsOfMatch(250, GREATER_THAN_OR_EQUAL), 1);
        assertEquals(partition.findAsOfMatch(350, GREATER_THAN_OR_EQUAL), 2);
    }
    
    @Test
    public void testGreaterThanVsGreaterThanOrEqual() {
        Page page = new Page(createLongsBlock(100, 200, 300));
        SortedPartition partition = new SortedPartition(ImmutableList.of(page), 0);
        
        // >= includes exact match
        assertEquals(partition.findAsOfMatch(200, GREATER_THAN_OR_EQUAL), 1);
        
        // > excludes exact match
        assertEquals(partition.findAsOfMatch(200, GREATER_THAN), 0);
    }
}
```

**Run tests to verify they fail:**
```bash
./mvnw test -pl :trino-main -Dtest=TestSortedPartition
# Expected: SortedPartition class does not exist
```

#### Step 2: Implement SortedPartition
Create minimal implementation to pass tests:

```java
public class SortedPartition {
    private final List<Page> pages;
    private final int timestampChannel;
    
    public int findAsOfMatch(long probeTimestamp, ComparisonOperator op) {
        // Implement binary search to make tests pass
        // Start with simple implementation, optimize later
    }
}
```

#### Step 3: Write Operator Tests
Create `TestAsofJoinOperator.java`:

```java
public class TestAsofJoinOperator {
    
    @Test
    public void testBasicInnerAsofJoin() {
        // Build side: quotes (symbol, time, price)
        List<Page> buildPages = ImmutableList.of(new Page(
            createStringsBlock("AAPL", "AAPL", "AAPL"),
            createLongsBlock(100, 200, 300),
            createDoublesBlock(150.0, 151.0, 152.0)));
        
        // Probe side: trades (symbol, time)
        Page probePage = new Page(
            createStringsBlock("AAPL", "AAPL"),
            createLongsBlock(250, 350));
        
        // Create operator
        OperatorFactory operatorFactory = createAsofJoinOperatorFactory(
            buildPages, 
            false); // inner join
        
        // Process probe page
        MaterializedResult result = operatorFactory.createOperator(driverContext)
            .process(probePage)
            .toMaterializedResult();
        
        // Verify results
        assertEquals(result.getRowCount(), 2);
        
        // First trade at 250 matches quote at 200
        assertEquals(result.getMaterializedRows().get(0).getField(2), 151.0);
        
        // Second trade at 350 matches quote at 300  
        assertEquals(result.getMaterializedRows().get(1).getField(2), 152.0);
    }
    
    @Test
    public void testLeftOuterAsofJoin() {
        // Test with unmatched probe rows
        List<Page> buildPages = ImmutableList.of(new Page(
            createStringsBlock("AAPL"),
            createLongsBlock(100)));
        
        Page probePage = new Page(
            createStringsBlock("AAPL", "GOOG"),
            createLongsBlock(200, 150));
        
        MaterializedResult result = runAsofJoin(buildPages, probePage, true); // left join
        
        assertEquals(result.getRowCount(), 2);
        
        // AAPL matches
        assertNotNull(result.getMaterializedRows().get(0).getField(2));
        
        // GOOG doesn't match - should have nulls
        assertNull(result.getMaterializedRows().get(1).getField(2));
    }
    
    @Test
    public void testNullHandling() {
        // Test NULL timestamps
        Page probePage = new Page(
            createStringsBlock("AAPL"),
            createLongsBlock(NULL_VALUE));
        
        MaterializedResult result = runAsofJoin(buildPages, probePage, false);
        
        // NULL timestamps should not match
        assertEquals(result.getRowCount(), 0);
    }
}
```

#### Step 4: Write Performance Test
```java
@Test
public void testLargeScalePerformance() {
    // Create large dataset
    int buildRows = 1_000_000;
    int probeRows = 100_000;
    
    List<Page> buildPages = createTimeSeries("AAPL", buildRows);
    Page probePage = createRandomProbes("AAPL", probeRows);
    
    long startTime = System.nanoTime();
    MaterializedResult result = runAsofJoin(buildPages, probePage, false);
    long duration = System.nanoTime() - startTime;
    
    // Verify correctness
    assertEquals(result.getRowCount(), probeRows);
    
    // Verify performance (should use binary search)
    double timePerProbe = duration / (double) probeRows;
    assertTrue(timePerProbe < 1000, // nanoseconds
        "Binary search should be fast: " + timePerProbe + "ns per probe");
}
```

### Testing Verification Points
- Start with simple test cases, add complexity gradually
- Test edge cases (nulls, empty data, single row)
- Verify both correctness and performance
- Each test should drive a specific implementation detail

### Success Criteria
- Operator correctly performs temporal matching
- Binary search is used for efficiency
- NULL values handled correctly
- Left/right outer joins work properly
- Performance tests confirm O(log n) lookup time

---

## Agent Task 5: Memory Management and Spilling (TDD)

### Context
ASOF joins can consume significant memory when build-side data is large. The operator needs to integrate with Trino's memory management and support spilling to disk when memory limits are exceeded.

### Dependencies
- Agent Task 4 (Operator Implementation) must be complete

### Your Task
Add memory accounting and spilling support to the ASOF join operator using Test-Driven Development.

### Test-First Development Steps

#### Step 1: Write Memory Accounting Tests
Create `TestAsofJoinMemoryAccounting.java`:

```java
public class TestAsofJoinMemoryAccounting {
    
    @Test
    public void testMemoryTracking() {
        AsofJoinOperator operator = createOperatorWithMemoryLimit(DataSize.of(10, MEGABYTE));
        
        // Add pages and verify memory is tracked
        long initialMemory = operator.getMemoryReservation();
        
        operator.addInput(createLargePage(1000));
        
        long afterFirstPage = operator.getMemoryReservation();
        assertTrue(afterFirstPage > initialMemory);
        
        // Verify memory is released on close
        operator.close();
        assertEquals(operator.getMemoryReservation(), 0);
    }
    
    @Test
    public void testMemoryLimit() {
        // Create operator with small memory limit
        AsofJoinOperator operator = createOperatorWithMemoryLimit(DataSize.of(1, KILOBYTE));
        
        // Try to add data exceeding limit
        assertThatThrownBy(() -> {
            for (int i = 0; i < 1000; i++) {
                operator.addInput(createLargePage(100));
            }
        }).isInstanceOf(ExceededMemoryLimitException.class)
          .hasMessageContaining("Query exceeded per-node memory limit");
    }
    
    @Test
    public void testPeakMemoryTracking() {
        AsofJoinOperator operator = createOperator();
        
        // Add and remove data
        operator.addInput(createLargePage(1000));
        long peakAfterAdd = operator.getOperatorContext().getPeakMemoryReservation();
        
        operator.getOutput(); // Process and release memory
        
        // Peak should remain even after memory is released
        assertEquals(operator.getOperatorContext().getPeakMemoryReservation(), peakAfterAdd);
    }
}
```

#### Step 2: Write Spilling Tests
Create `TestAsofJoinSpilling.java`:

```java
public class TestAsofJoinSpilling {
    
    @Test
    public void testSpillTriggeredByMemoryLimit() {
        // Create operator that will spill
        SpillableAsofJoinOperator operator = createSpillableOperator(
            DataSize.of(1, MEGABYTE), // memory limit
            spillerFactory);
        
        // Add data exceeding memory limit
        for (int i = 0; i < 100; i++) {
            operator.addInput(createPageWithSize(100_000));
        }
        
        // Verify spilling occurred
        assertTrue(operator.getSpilledBytes() > 0);
        assertTrue(operator.getSpilledPartitions() > 0);
        
        // Verify operator still produces correct results
        MaterializedResult result = operator.finish();
        verifyResults(result);
    }
    
    @Test
    public void testSpillAndRestore() {
        SpillableAsofLookupSource lookupSource = new SpillableAsofLookupSource(
            memoryContext, spillerFactory);
        
        // Add partitions
        SortedPartition partition1 = createPartition("AAPL", 1000);
        lookupSource.addPartition(1, partition1);
        
        // Spill partition
        lookupSource.spill(SpillReason.MEMORY_LIMIT);
        
        // Verify partition is no longer in memory
        assertFalse(lookupSource.isInMemory(1));
        
        // Access partition - should restore from disk
        SortedPartition restored = lookupSource.getPartition(1);
        assertNotNull(restored);
        assertEquals(restored.getPositionCount(), partition1.getPositionCount());
    }
    
    @Test
    public void testSelectiveSpilling() {
        // Test that we spill largest partitions first
        SpillableAsofLookupSource lookupSource = createLookupSource();
        
        // Add partitions of different sizes
        lookupSource.addPartition(1, createPartition(100));  // small
        lookupSource.addPartition(2, createPartition(10000)); // large
        lookupSource.addPartition(3, createPartition(500));   // medium
        
        // Trigger partial spill
        lookupSource.spillPartialMemory(0.5); // spill 50%
        
        // Verify largest partition was spilled
        assertFalse(lookupSource.isInMemory(2));
        assertTrue(lookupSource.isInMemory(1));
    }
}
```

#### Step 3: Implement Memory Context
Only after tests fail, create implementation:

```java
public class AsofJoinMemoryContext {
    // Implement methods needed to pass tests
    
    public boolean tryReserveMemory(long bytes) {
        // Start simple, add complexity as tests require
    }
}
```

#### Step 4: Integration Test
```java
@Test
public void testEndToEndWithSpilling() {
    // Large dataset that requires spilling
    TestingTaskContext taskContext = createTaskContext(DataSize.of(10, MEGABYTE));
    
    // Run full join
    List<Page> output = runAsofJoinWithSpilling(
        createLargeBuildSide(1_000_000),
        createLargeProbeSide(100_000),
        taskContext);
    
    // Verify results are correct despite spilling
    verifyJoinResults(output);
    
    // Verify spilling statistics
    OperatorStats stats = taskContext.getOperatorStats();
    assertTrue(stats.getSpilledDataSize().toBytes() > 0);
}
```

### Testing Verification Points
- Test memory tracking before implementing
- Verify spilling behavior with small memory limits
- Test correctness after spill/restore cycle
- Measure performance impact of spilling

### Success Criteria
- Memory usage is accurately tracked
- Operator spills when memory limit exceeded
- Spilled data can be restored correctly
- No memory leaks in spill/restore cycle
- Performance degradation is acceptable

---

## Agent Task 6: Query Optimization Rules (TDD)

### Context
ASOF joins can benefit from various optimizations like predicate pushdown, join reordering, and recognizing pre-sorted data. These optimizations can significantly improve query performance.

### Dependencies
- Agent Task 3 (Plan Node) must be complete

### Your Task
Implement optimizer rules specific to ASOF joins using Test-Driven Development.

### Test-First Development Steps

#### Step 1: Write Predicate Pushdown Tests
Create `TestAsofJoinPushdown.java`:

```java
public class TestAsofJoinPushdown {
    
    @Test
    public void testPushFilterThroughAsofJoin() {
        // Original plan: Filter on top of ASOF join
        PlanNode original = filter("b.exchange = 'NYSE'",
            asofJoin(
                tableScan("a", "symbol", "ts"),
                tableScan("b", "symbol", "ts", "exchange")));
        
        // Run optimizer
        PlanNode optimized = optimize(original);
        
        // Verify filter pushed to build side
        assertPlan(optimized,
            asofJoin(
                tableScan("a"),
                filter("exchange = 'NYSE'",
                    tableScan("b"))));
    }
    
    @Test
    public void testDontPushProbeFilter() {
        // Filter on probe side should not be pushed
        PlanNode original = filter("a.volume > 1000",
            asofJoin(
                tableScan("a", "symbol", "ts", "volume"),
                tableScan("b")));
        
        PlanNode optimized = optimize(original);
        
        // Filter should remain above join
        assertPlan(optimized,
            filter("a.volume > 1000",
                asofJoin(tableScan("a"), tableScan("b"))));
    }
    
    @Test
    public void testPushComplexFilter() {
        // Test with AND conditions
        PlanNode original = filter("a.symbol = 'AAPL' AND b.exchange = 'NYSE'",
            asofJoin(tableScan("a"), tableScan("b")));
        
        PlanNode optimized = optimize(original);
        
        // Both filters pushed appropriately
        assertPlan(optimized,
            filter("a.symbol = 'AAPL'",
                asofJoin(
                    tableScan("a"),
                    filter("exchange = 'NYSE'", tableScan("b")))));
    }
}
```

#### Step 2: Write Pre-sorted Detection Tests
```java
public class TestAsofJoinPresortedOptimization {
    
    @Test
    public void testRecognizePresortedTable() {
        // Table with sort property
        TableScanNode sortedScan = tableScan("sorted_quotes",
            ImmutableMap.of("sorted_by", ImmutableList.of("ts")));
        
        PlanNode original = asofJoin(
            tableScan("trades"),
            sortedScan);
        
        PlanNode optimized = optimize(original);
        
        // Verify no sort is added
        assertThat(searchFrom(optimized).where(SortNode.class::isInstance).findAll())
            .isEmpty();
        
        // Verify property is set
        AsofJoinNode optimizedJoin = getOnlyElement(
            searchFrom(optimized).where(AsofJoinNode.class::isInstance).findAll());
        assertTrue(optimizedJoin.isPresortedBuild());
    }
    
    @Test
    public void testAddSortWhenNeeded() {
        PlanNode original = asofJoin(
            tableScan("trades"),
            tableScan("unsorted_quotes"));
        
        PlanNode optimized = optimize(original);
        
        // Verify sort is added to build side
        assertPlan(optimized,
            asofJoin(
                tableScan("trades"),
                sort("ts ASC", tableScan("unsorted_quotes"))));
    }
}
```

#### Step 3: Write Statistics Tests
```java
public class TestAsofJoinStatsCalculator {
    
    @Test
    public void testInnerJoinStats() {
        PlanNodeStatsEstimate probeStats = PlanNodeStatsEstimate.builder()
            .setOutputRowCount(1000)
            .addSymbolStatistics(new Symbol("ts"), 
                SymbolStatsEstimate.builder()
                    .setLowValue(100)
                    .setHighValue(200)
                    .build())
            .build();
        
        PlanNodeStatsEstimate buildStats = PlanNodeStatsEstimate.builder()
            .setOutputRowCount(10000)
            .addSymbolStatistics(new Symbol("ts"),
                SymbolStatsEstimate.builder()
                    .setLowValue(0)
                    .setHighValue(300)
                    .build())
            .build();
        
        PlanNodeStatsEstimate result = AsofJoinStatsCalculator.calculate(
            probeStats, buildStats, INNER_JOIN);
        
        // Each probe row matches at most one build row
        assertTrue(result.getOutputRowCount() <= probeStats.getOutputRowCount());
        
        // Some probe rows might not match
        assertTrue(result.getOutputRowCount() > 0);
    }
    
    @Test
    public void testLeftJoinStats() {
        // Left join preserves all probe rows
        PlanNodeStatsEstimate result = AsofJoinStatsCalculator.calculate(
            probeStats, buildStats, LEFT_JOIN);
        
        assertEquals(result.getOutputRowCount(), probeStats.getOutputRowCount());
    }
}
```

### Testing Verification Points
- Each optimization rule tested in isolation
- Test both positive and negative cases
- Verify optimizations preserve correctness
- Test interaction between multiple rules

### Success Criteria
- Predicates pushed through ASOF joins appropriately
- Pre-sorted data recognized and utilized
- Statistics estimation reasonable and tested
- All optimizations preserve query semantics

---

## Agent Task 7: Comprehensive Testing Suite (TDD)

### Context
ASOF joins need thorough testing to ensure correctness, performance, and compatibility with other Trino features.

### Dependencies
- All previous implementation tasks should be complete

### Your Task
Create a comprehensive test suite covering unit tests, integration tests, and performance benchmarks using Test-Driven Development.

### Test-First Development Steps

#### Step 1: Write Correctness Verification Tests
Create `TestAsofJoinCorrectness.java`:

```java
public class TestAsofJoinCorrectness extends AbstractTestQueryFramework {
    
    @BeforeClass
    public void setupTestData() {
        // This will fail until ASOF join is fully implemented
        assertUpdate("CREATE TABLE trades (symbol VARCHAR, ts BIGINT, price DOUBLE)");
        assertUpdate("CREATE TABLE quotes (symbol VARCHAR, ts BIGINT, bid DOUBLE, ask DOUBLE)");
        
        // Insert test data
        assertUpdate("INSERT INTO trades VALUES " +
            "('AAPL', 100, 150.5), " +
            "('AAPL', 200, 151.0), " +
            "('GOOG', 150, 2000.0)");
            
        assertUpdate("INSERT INTO quotes VALUES " +
            "('AAPL', 50, 150.0, 150.2), " +
            "('AAPL', 150, 151.0, 151.2), " +
            "('AAPL', 250, 152.0, 152.2), " +
            "('GOOG', 100, 1999.0, 2001.0)");
    }
    
    @Test
    public void testAsofJoinVsWindowFunction() {
        // ASOF JOIN query
        String asofQuery = """
            SELECT t.symbol, t.ts, t.price, q.bid, q.ask
            FROM trades t
            ASOF LEFT JOIN quotes q
            ON t.symbol = q.symbol AND t.ts >= q.ts
            ORDER BY t.symbol, t.ts
            """;
        
        // Equivalent window function query
        String windowQuery = """
            WITH matched AS (
                SELECT 
                    t.symbol, t.ts, t.price, q.bid, q.ask,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.symbol, t.ts 
                        ORDER BY q.ts DESC
                    ) as rn
                FROM trades t
                LEFT JOIN quotes q
                ON t.symbol = q.symbol AND t.ts >= q.ts
            )
            SELECT symbol, ts, price, bid, ask
            FROM matched
            WHERE rn = 1 OR bid IS NULL
            ORDER BY symbol, ts
            """;
        
        // Results should be identical
        assertQuery(asofQuery, windowQuery);
    }
    
    @Test
    public void testEdgeCases() {
        // Empty build side
        assertQuery(
            "SELECT * FROM trades ASOF LEFT JOIN empty_quotes q ON trades.symbol = q.symbol",
            "SELECT trades.*, NULL, NULL FROM trades");
        
        // NULL temporal values
        assertUpdate("INSERT INTO trades VALUES ('X', NULL, 100.0)");
        assertQuery(
            "SELECT COUNT(*) FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts WHERE t.ts IS NULL",
            "VALUES (0)");
        
        // Duplicate timestamps
        assertUpdate("INSERT INTO quotes VALUES ('AAPL', 150, 151.5, 151.7)");
        MaterializedResult result = computeActual(
            "SELECT q.bid FROM trades t ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts WHERE t.ts = 150");
        // Should consistently pick one (last after stable sort)
        assertEquals(result.getOnlyValue(), 151.5);
    }
}
```

#### Step 2: Write Performance Benchmarks
Create `BenchmarkAsofJoin.java`:

```java
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BenchmarkAsofJoin {
    
    @Benchmark
    public List<Page> benchmarkAsofJoin(BenchmarkData data) {
        return runQuery("""
            SELECT t.*, q.price
            FROM trades_%s t
            ASOF JOIN quotes_%s q
            ON t.symbol = q.symbol AND t.ts >= q.ts
            """.formatted(data.scale, data.scale));
    }
    
    @Benchmark
    public List<Page> benchmarkWindowFunction(BenchmarkData data) {
        return runQuery("""
            WITH matched AS (
                SELECT t.*, q.price,
                    ROW_NUMBER() OVER (PARTITION BY t.trade_id ORDER BY q.ts DESC) as rn
                FROM trades_%s t
                LEFT JOIN quotes_%s q
                ON t.symbol = q.symbol AND t.ts >= q.ts
            )
            SELECT * FROM matched WHERE rn = 1
            """.formatted(data.scale, data.scale));
    }
    
    @State(Scope.Benchmark)
    public static class BenchmarkData {
        @Param({"small", "medium", "large"})
        private String scale;
        
        @Setup
        public void setup() {
            createScaledTestData(scale);
        }
    }
}
```

Run benchmarks:
```bash
./mvnw test -pl :trino-benchmarks -Dtest=BenchmarkAsofJoin
# Verify ASOF is significantly faster than window function
```

#### Step 3: Write Distributed Execution Tests
```java
public class TestAsofJoinDistributed extends AbstractTestDistributedQueries {
    
    @Test
    public void testDataLocalityOptimization() {
        // Ensure data is distributed
        assertUpdate("""
            CREATE TABLE distributed_trades 
            WITH (partitioned_by = ARRAY['dt'])
            AS SELECT *, DATE(FROM_UNIXTIME(ts/1000)) as dt
            FROM trades
            """);
        
        // Run distributed ASOF join
        MaterializedResult result = computeActual("""
            SELECT COUNT(*)
            FROM distributed_trades t
            ASOF JOIN distributed_quotes q
            ON t.symbol = q.symbol AND t.ts >= q.ts
            """);
        
        // Verify execution stats
        QueryStats stats = getQueryStats();
        
        // Should use local joins when possible
        assertTrue(stats.getPhysicalInputPositions() < 
                  stats.getLogicalInputPositions() * 2);
    }
    
    @Test
    public void testSkewHandling() {
        // Create skewed data - 90% AAPL
        createSkewedData();
        
        // Should still complete efficiently
        assertQuerySucceeds("""
            SELECT AVG(q.price - t.price)
            FROM skewed_trades t
            ASOF JOIN skewed_quotes q
            ON t.symbol = q.symbol AND t.ts >= q.ts
            """);
        
        // Verify no single node is overloaded
        verifyBalancedExecution();
    }
}
```

### Testing Verification Points
- Write tests that will fail without implementation
- Cover all edge cases and error conditions
- Benchmark against alternative approaches
- Test at scale with distributed execution

### Success Criteria
- All correctness tests pass
- Performance is 10x+ better than alternatives
- Handles edge cases gracefully
- Scales to billions of rows
- No performance regressions

---

## Agent Task 8: Documentation and Examples (TDD)

### Context
Good documentation is crucial for adoption. Users need clear explanations of ASOF join semantics, syntax, and best practices.

### Dependencies
- Basic implementation should be complete

### Your Task
Create comprehensive documentation for ASOF joins in Trino using Test-Driven Development for code examples.

### Test-First Development Steps

#### Step 1: Write Documentation Example Tests
Create `TestDocumentationExamples.java`:

```java
public class TestDocumentationExamples {
    
    @Test
    public void testBasicAsofJoinExample() {
        // This example will be used in docs
        String example = """
            SELECT 
                t.trade_id,
                t.symbol,
                t.quantity,
                t.trade_time,
                q.bid,
                q.ask,
                q.quote_time
            FROM trades t
            ASOF JOIN quotes q 
            ON t.symbol = q.symbol 
            AND t.trade_time >= q.quote_time
            """;
        
        // Verify it actually works
        assertQuerySucceeds(example);
        
        // Verify output format for docs
        MaterializedResult result = computeActual(example + " LIMIT 1");
        assertEquals(result.getColumnCount(), 7);
    }
    
    @Test
    public void testMigrationExamples() {
        // Window function version
        String windowVersion = """
            WITH latest_quotes AS (
                SELECT 
                    t.trade_id,
                    q.price,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.trade_id 
                        ORDER BY q.timestamp DESC
                    ) as rn
                FROM trades t
                JOIN quotes q
                ON t.symbol = q.symbol
                AND t.timestamp >= q.timestamp
            )
            SELECT * FROM latest_quotes WHERE rn = 1
            """;
        
        // ASOF version
        String asofVersion = """
            SELECT t.trade_id, q.price
            FROM trades t
            ASOF JOIN quotes q
            ON t.symbol = q.symbol
            AND t.timestamp >= q.timestamp
            """;
        
        // Verify both work and produce same results
        assertQuery(asofVersion, windowVersion);
    }
    
    @Test
    public void testPerformanceTuningExamples() {
        // Pre-sorted table example
        assertUpdate("""
            CREATE TABLE sorted_quotes AS
            SELECT * FROM quotes
            ORDER BY symbol, timestamp
            """);
        
        // Verify optimization is applied
        String plan = getExplainPlan("""
            SELECT * FROM trades
            ASOF JOIN sorted_quotes
            ON trades.symbol = sorted_quotes.symbol
            AND trades.ts >= sorted_quotes.ts
            """);
        
        assertFalse(plan.contains("Sort"));
    }
}
```

#### Step 2: Create Documentation Structure
Write documentation with embedded test verification:

```markdown
<!-- docs/src/main/sphinx/sql/asof-join.rst -->

ASOF JOIN
=========

<!-- TEST: TestDocumentationExamples.testBasicAsofJoinExample -->
Synopsis::

    SELECT * FROM left_table
    ASOF [LEFT | RIGHT] JOIN right_table
    ON equality_condition AND temporal_condition

<!-- END TEST -->
```

#### Step 3: Write Interactive Examples
Create runnable examples:

```java
@Test
public void generateInteractiveExamples() {
    // Generate sample data for docs
    String setupScript = """
        -- Create sample trading data
        CREATE TABLE trades (
            trade_id BIGINT,
            symbol VARCHAR,
            quantity INTEGER,
            trade_time TIMESTAMP
        );
        
        CREATE TABLE quotes (
            symbol VARCHAR,
            bid DECIMAL(10,2),
            ask DECIMAL(10,2),
            quote_time TIMESTAMP
        );
        
        -- Insert sample data
        INSERT INTO trades VALUES
            (1, 'AAPL', 100, TIMESTAMP '2023-01-01 09:30:00'),
            (2, 'AAPL', 200, TIMESTAMP '2023-01-01 09:30:05'),
            (3, 'GOOG', 50,  TIMESTAMP '2023-01-01 09:30:03');
            
        INSERT INTO quotes VALUES
            ('AAPL', 150.00, 150.05, TIMESTAMP '2023-01-01 09:29:55'),
            ('AAPL', 150.10, 150.15, TIMESTAMP '2023-01-01 09:30:02'),
            ('GOOG', 2800.00, 2800.50, TIMESTAMP '2023-01-01 09:30:00');
        """;
    
    // Verify setup works
    executeSqlScript(setupScript);
    
    // Save as docs/examples/asof-join-setup.sql
    writeToFile("docs/examples/asof-join-setup.sql", setupScript);
}
```

### Testing Verification Points
- Every code example in documentation is tested
- Migration examples show identical results
- Performance claims are verified by benchmarks
- Examples use realistic data

### Success Criteria
- All documentation examples pass tests
- Examples cover common use cases
- Performance tuning guide backed by benchmarks
- Migration guide tested for correctness
- Documentation integrated with CI/CD

---

## Implementation Order and Dependencies

### Phase 1 - Core Implementation (Can be parallelized)
- **Agent Task 1**: Parser Support (no dependencies)
- **Agent Task 2**: Analyzer Support (depends on Task 1)
- **Agent Task 3**: Plan Node (depends on Task 2)

### Phase 2 - Execution (Sequential)
- **Agent Task 4**: Operator Implementation (depends on Task 3)
- **Agent Task 5**: Memory Management (depends on Task 4)

### Phase 3 - Optimization & Polish (Can be parallelized)
- **Agent Task 6**: Query Optimization (depends on Task 3)
- **Agent Task 7**: Testing Suite (depends on all implementation tasks)
- **Agent Task 8**: Documentation (can start early with failing tests)

## TDD Guidelines for All Agents

1. **Red Phase**: Write a failing test that defines desired behavior
2. **Green Phase**: Write minimal code to make the test pass
3. **Refactor Phase**: Improve code quality while keeping tests green

### Test Organization
```
src/test/java/
├── unit/           # Fast, isolated tests
├── integration/    # Cross-component tests
├── performance/    # Benchmark tests
└── examples/       # Documentation example tests
```

### Running Tests
```bash
# Run specific test
./mvnw test -pl :module-name -Dtest=TestClassName#methodName

# Run all tests for a module
./mvnw test -pl :module-name

# Run with coverage
./mvnw test -pl :module-name jacoco:report

# Run only unit tests (fast)
./mvnw test -pl :module-name -Dgroups=unit
```

## Notes for Agents

- **Always write tests first** - This is non-negotiable
- Each test should test ONE thing
- Test names should describe what they verify
- Use meaningful assertions with helpful messages
- Keep tests fast - mock external dependencies
- If a test is hard to write, the design might need improvement
- Run tests frequently during development
- Don't commit code with failing tests