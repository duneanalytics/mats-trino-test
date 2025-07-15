# ASOF Join Implementation Plan for Distributed Coding Agents

## Overview

This document contains self-contained implementation tasks for adding ASOF JOIN support to Trino. Each task is designed to be completed independently by a coding agent with all necessary context included.

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
Add parser support for the ASOF keyword and join type in Trino's SQL grammar.

### Implementation Steps

1. **Update SQL Grammar** (`trino-parser/src/main/antlr4/io/trino/grammar/sql/SqlBase.g4`):
   - Add `ASOF` to the `nonReserved` rule (around line 2000+)
   - Update the `joinType` rule to include ASOF: `(INNER | LEFT | RIGHT | FULL | CROSS | ASOF)?`
   - Ensure ASOF can be combined with LEFT/RIGHT for outer joins: `ASOF LEFT JOIN`

2. **Update Join AST Node** (`trino-parser/src/main/java/io/trino/sql/tree/Join.java`):
   - Add `ASOF` to the `Type` enum
   - Update any switch statements that handle join types
   - Ensure toString() method handles ASOF type

3. **Update AST Visitor** (`trino-parser/src/main/java/io/trino/sql/tree/AstVisitor.java`):
   - No changes needed if using existing visitJoin method

4. **Add Parser Tests** (`trino-parser/src/test/java/io/trino/sql/parser/TestSqlParser.java`):
   ```java
   @Test
   public void testAsofJoin() {
       // Basic ASOF JOIN
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
       
       // ASOF LEFT JOIN
       assertStatement("SELECT * FROM trades ASOF LEFT JOIN quotes ON trades.symbol = quotes.symbol AND trades.time >= quotes.time",
           /* similar structure with Join.Type.ASOF and isOuter flag */);
   }
   ```

5. **Update SQL Formatter** (`trino-parser/src/main/java/io/trino/sql/SqlFormatter.java`):
   - In `visitJoin` method, add case for ASOF join type formatting

### Testing Requirements
- Parser correctly recognizes ASOF keyword
- ASOF JOIN and ASOF LEFT/RIGHT JOIN are parsed correctly  
- Invalid syntax (e.g., ASOF FULL JOIN) produces appropriate errors
- Round-trip through parser and formatter preserves ASOF syntax

### Success Criteria
- `./mvnw test -pl :trino-parser` passes all tests
- New ASOF join queries can be parsed without errors
- AST correctly represents ASOF join type

---

## Agent Task 2: Query Analyzer Support for ASOF JOIN

### Context
ASOF joins require special validation - they must have exactly one temporal condition (>= or >) and zero or more equality conditions. The analyzer must extract these conditions separately for the planner.

### Dependencies
- Agent Task 1 (Parser Support) must be complete

### Your Task
Add analyzer support to validate ASOF join syntax and extract equality/temporal criteria.

### Implementation Steps

1. **Create ASOF Join Validator** (`trino-main/src/main/java/io/trino/sql/analyzer/AsofJoinAnalyzer.java`):
   ```java
   public class AsofJoinAnalyzer {
       public static AsofJoinCriteria analyzeAsofJoin(Expression criteria, Analysis analysis) {
           // Extract equality and temporal conditions from join criteria
           // Validate exactly one temporal condition (>= or >)
           // Return structured criteria for planner
       }
       
       public static class AsofJoinCriteria {
           private final List<Expression> equalityConditions;
           private final Expression leftTemporal;
           private final Expression rightTemporal; 
           private final ComparisonOperator temporalOperator; // >= or >
       }
   }
   ```

2. **Update StatementAnalyzer** (`trino-main/src/main/java/io/trino/sql/analyzer/StatementAnalyzer.java`):
   ```java
   @Override
   protected Scope visitJoin(Join node, Scope scope) {
       // ... existing code ...
       
       if (node.getType() == Join.Type.ASOF) {
           if (node.getCriteria().isEmpty()) {
               throw semanticException(MISSING_ASOF_CRITERIA, node, "ASOF join requires join criteria");
           }
           
           if (node.getCriteria().get() instanceof JoinOn) {
               JoinOn joinOn = (JoinOn) node.getCriteria().get();
               AsofJoinCriteria criteria = AsofJoinAnalyzer.analyzeAsofJoin(
                   joinOn.getExpression(), analysis);
               
               // Store criteria in analysis for planner
               analysis.setAsofJoinCriteria(node, criteria);
           } else {
               throw semanticException(INVALID_ASOF_JOIN, node, 
                   "ASOF join does not support USING clause");
           }
       }
       
       // ... rest of existing join handling ...
   }
   ```

3. **Update Analysis class** (`trino-main/src/main/java/io/trino/sql/analyzer/Analysis.java`):
   - Add field: `private final Map<Join, AsofJoinCriteria> asofJoinCriteria = new HashMap<>();`
   - Add methods: `setAsofJoinCriteria()` and `getAsofJoinCriteria()`

4. **Add Semantic Exceptions** (`trino-main/src/main/java/io/trino/spi/StandardErrorCode.java`):
   ```java
   MISSING_ASOF_CRITERIA(67, USER_ERROR),
   INVALID_ASOF_JOIN(68, USER_ERROR),
   MULTIPLE_TEMPORAL_CONDITIONS(69, USER_ERROR),
   ```

5. **Add Analyzer Tests** (`trino-main/src/test/java/io/trino/sql/analyzer/TestStatementAnalyzer.java`):
   ```java
   @Test
   public void testAsofJoinValidation() {
       // Valid ASOF join
       analyze("SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a AND t1.ts >= t2.ts");
       
       // Missing temporal condition
       assertFails("SELECT * FROM t1 ASOF JOIN t2 ON t1.a = t2.a")
           .hasErrorCode(MISSING_ASOF_CRITERIA);
       
       // Multiple temporal conditions
       assertFails("SELECT * FROM t1 ASOF JOIN t2 ON t1.ts >= t2.ts AND t1.ts2 >= t2.ts2")
           .hasErrorCode(MULTIPLE_TEMPORAL_CONDITIONS);
       
       // Invalid temporal operator
       assertFails("SELECT * FROM t1 ASOF JOIN t2 ON t1.ts <= t2.ts")
           .hasErrorCode(INVALID_ASOF_JOIN);
   }
   ```

### Success Criteria
- ASOF joins are validated for correct temporal conditions
- Equality and temporal criteria are extracted and stored in Analysis
- Invalid ASOF join syntax produces clear error messages
- All analyzer tests pass

---

## Agent Task 3: Plan Node for ASOF JOIN

### Context
The planner needs a dedicated plan node to represent ASOF joins in the query plan. This node will store the logical representation before being translated to physical operators.

### Dependencies
- Agent Task 2 (Analyzer Support) must be complete

### Your Task
Create the AsofJoinNode plan node and integrate it with the query planner.

### Implementation Steps

1. **Create AsofJoinNode** (`trino-main/src/main/java/io/trino/sql/planner/plan/AsofJoinNode.java`):
   ```java
   public class AsofJoinNode extends PlanNode {
       private final PlanNode left;
       private final PlanNode right;
       private final List<EquiJoinClause> equalityCriteria;
       private final Symbol leftTimeSymbol;
       private final Symbol rightTimeSymbol;
       private final ComparisonOperator temporalOperator; // >= or >
       private final boolean isLeftJoin;
       private final boolean isRightJoin;
       private final Optional<Symbol> leftHashSymbol;
       private final Optional<Symbol> rightHashSymbol;
       private final JoinDistributionType distributionType;
       
       @Override
       public List<Symbol> getOutputSymbols() {
           return ImmutableList.<Symbol>builder()
               .addAll(left.getOutputSymbols())
               .addAll(isLeftJoin || isRightJoin ? 
                   right.getOutputSymbols().stream()
                       .map(symbol -> new Symbol(symbol.getName()))
                       .collect(toList()) : 
                   right.getOutputSymbols())
               .build();
       }
       
       @Override
       public PlanNode accept(PlanVisitor<?, ?> visitor, Object context) {
           return visitor.visitAsofJoin(this, context);
       }
       
       @Override
       public PlanNode replaceChildren(List<PlanNode> newChildren) {
           return new AsofJoinNode(/* ... */);
       }
   }
   ```

2. **Update PlanVisitor** (`trino-main/src/main/java/io/trino/sql/planner/plan/PlanVisitor.java`):
   ```java
   public R visitAsofJoin(AsofJoinNode node, C context) {
       return visitPlan(node, context);
   }
   ```

3. **Update RelationPlanner** (`trino-main/src/main/java/io/trino/sql/planner/RelationPlanner.java`):
   ```java
   @Override
   protected RelationPlan visitJoin(Join node, Void context) {
       // ... existing code ...
       
       if (node.getType() == Join.Type.ASOF) {
           AsofJoinCriteria criteria = analysis.getAsofJoinCriteria(node);
           
           // Plan left and right sides
           RelationPlan left = process(node.getLeft(), context);
           RelationPlan right = process(node.getRight(), context);
           
           // Convert criteria to symbols
           List<EquiJoinClause> equalityClauses = /* extract equality conditions */;
           Symbol leftTime = /* extract left temporal symbol */;
           Symbol rightTime = /* extract right temporal symbol */;
           
           AsofJoinNode asofNode = new AsofJoinNode(
               idAllocator.getNextId(),
               left.getRoot(),
               right.getRoot(),
               equalityClauses,
               leftTime,
               rightTime,
               criteria.getTemporalOperator(),
               node.getType().name().contains("LEFT"),
               node.getType().name().contains("RIGHT"),
               Optional.empty(), // hash symbols added by optimizer
               Optional.empty(),
               JoinDistributionType.PARTITIONED);
           
           return new RelationPlan(
               asofNode,
               analysis.getScope(node),
               computeOutputs(asofNode));
       }
   }
   ```

4. **Add to PlanPrinter** (`trino-main/src/main/java/io/trino/sql/planner/planprinter/PlanPrinter.java`):
   ```java
   @Override
   public Void visitAsofJoin(AsofJoinNode node, Context context) {
       context.addNode(node,
           "AsofJoin[%s]".formatted(node.getTemporalOperator()),
           ImmutableMap.of(
               "Left", node.getLeft().getId(),
               "Right", node.getRight().getId(),
               "Equality", formatEquiJoinClauses(node.getEqualityCriteria()),
               "Temporal", "%s %s %s".formatted(
                   node.getLeftTimeSymbol(),
                   node.getTemporalOperator(),
                   node.getRightTimeSymbol())),
           node.getLeft(), node.getRight());
       return null;
   }
   ```

5. **Add Plan Tests** (`trino-main/src/test/java/io/trino/sql/planner/TestAsofJoinPlanning.java`):
   ```java
   @Test
   public void testAsofJoinPlan() {
       Plan plan = plan("SELECT * FROM trades t ASOF JOIN quotes q " +
                       "ON t.symbol = q.symbol AND t.ts >= q.ts",
                       LogicalPlanner.Stage.CREATED);
       
       PlanNode root = plan.getRoot();
       AsofJoinNode asofJoin = searchFrom(root)
           .where(AsofJoinNode.class::isInstance)
           .findOnlyElement();
       
       assertEquals(asofJoin.getTemporalOperator(), GREATER_THAN_OR_EQUAL);
       assertEquals(asofJoin.getEqualityCriteria().size(), 1);
   }
   ```

### Success Criteria
- AsofJoinNode correctly represents ASOF join in query plan
- EXPLAIN shows ASOF join details clearly
- Plan can be created for various ASOF join queries
- Tests verify correct plan structure

---

## Agent Task 4: ASOF Join Operator Implementation

### Context
The operator is responsible for the actual execution of ASOF joins. It needs to efficiently match each probe row with the most recent build row using binary search on sorted partitions.

### Dependencies
- Agent Task 3 (Plan Node) must be complete

### Your Task
Implement the core ASOF join operator that performs the temporal matching logic.

### Implementation Steps

1. **Create AsofJoinOperator** (`trino-main/src/main/java/io/trino/operator/join/AsofJoinOperator.java`):
   ```java
   public class AsofJoinOperator implements Operator {
       private final OperatorContext operatorContext;
       private final AsofLookupSource lookupSource;
       private final int[] probeChannels;
       private final int probeTimeChannel;
       private final JoinProbeFactory probeFactory;
       private final PageBuilder pageBuilder;
       private final boolean isLeftJoin;
       
       @Override
       public void addInput(Page page) {
           for (int position = 0; position < page.getPositionCount(); position++) {
               // Hash probe row to find partition
               long hash = hashStrategy.hashRow(probeChannels, page, position);
               
               // Get sorted partition for this hash
               SortedPartition partition = lookupSource.getPartition(hash);
               
               if (partition != null) {
                   // Extract probe timestamp
                   long probeTime = page.getLong(probeTimeChannel, position);
                   
                   // Binary search for matching position
                   int matchPosition = partition.findAsOfMatch(probeTime, temporalOperator);
                   
                   if (matchPosition >= 0) {
                       // Output joined row
                       outputJoinedRow(page, position, partition, matchPosition);
                   } else if (isLeftJoin) {
                       // Output probe row with nulls for build side
                       outputUnmatchedRow(page, position);
                   }
               } else if (isLeftJoin) {
                   outputUnmatchedRow(page, position);
               }
           }
       }
   }
   ```

2. **Create SortedPartition** (`trino-main/src/main/java/io/trino/operator/join/SortedPartition.java`):
   ```java
   public class SortedPartition {
       private final List<Page> pages;
       private final int[] positionOffsets; // Cumulative position count per page
       private final int timestampChannel;
       private final int totalPositions;
       
       public int findAsOfMatch(long probeTimestamp, ComparisonOperator op) {
           int low = 0;
           int high = totalPositions - 1;
           int result = -1;
           
           while (low <= high) {
               int mid = (low + high) >>> 1;
               long buildTimestamp = getTimestamp(mid);
               
               if (op == GREATER_THAN_OR_EQUAL) {
                   if (buildTimestamp <= probeTimestamp) {
                       result = mid;  // Potential match
                       low = mid + 1; // Look for later timestamp
                   } else {
                       high = mid - 1;
                   }
               } else { // GREATER_THAN
                   if (buildTimestamp < probeTimestamp) {
                       result = mid;
                       low = mid + 1;
                   } else {
                       high = mid - 1;
                   }
               }
           }
           
           return result;
       }
       
       private long getTimestamp(int absolutePosition) {
           // Find page containing this position
           int pageIndex = findPageIndex(absolutePosition);
           int positionInPage = absolutePosition - 
               (pageIndex > 0 ? positionOffsets[pageIndex - 1] : 0);
           
           return pages.get(pageIndex).getLong(timestampChannel, positionInPage);
       }
   }
   ```

3. **Create AsofJoinOperatorFactory** (`trino-main/src/main/java/io/trino/operator/join/AsofJoinOperatorFactory.java`):
   ```java
   public class AsofJoinOperatorFactory implements OperatorFactory {
       private final int operatorId;
       private final PlanNodeId planNodeId;
       private final JoinBridgeManager<AsofJoinBridge> joinBridgeManager;
       private final List<Type> probeTypes;
       private final List<Integer> probeJoinChannels;
       private final int probeTimeChannel;
       private final OptionalInt probeHashChannel;
       private final List<Type> buildOutputTypes;
       
       @Override
       public Operator createOperator(DriverContext driverContext) {
           AsofJoinBridge bridge = joinBridgeManager.getJoinBridge();
           return new AsofJoinOperator(/* parameters */);
       }
   }
   ```

4. **Create AsofHashBuilderOperator** (`trino-main/src/main/java/io/trino/operator/join/AsofHashBuilderOperator.java`):
   ```java
   public class AsofHashBuilderOperator extends HashBuilderOperator {
       private final int timeChannel;
       
       @Override
       public void finish() {
           if (lookupSourceFactory != null) {
               // Sort each partition by timestamp before finalizing
               ((AsofLookupSourceFactory) lookupSourceFactory).sortPartitions();
           }
           super.finish();
       }
   }
   ```

5. **Integrate with LocalExecutionPlanner** (`trino-main/src/main/java/io/trino/sql/planner/LocalExecutionPlanner.java`):
   ```java
   @Override
   public PhysicalOperation visitAsofJoin(AsofJoinNode node, LocalExecutionPlanContext context) {
       // Create build side operator
       AsofHashBuilderOperatorFactory buildOperatorFactory = new AsofHashBuilderOperatorFactory(
           /* configure with node parameters */);
       
       // Create probe side operator  
       AsofJoinOperatorFactory joinOperatorFactory = new AsofJoinOperatorFactory(
           /* configure with node parameters */);
       
       // Set up join bridge
       AsofJoinBridge joinBridge = new AsofJoinBridge();
       
       // Return physical operation
       return new PhysicalOperation(/* ... */);
   }
   ```

6. **Add Operator Tests** (`trino-main/src/test/java/io/trino/operator/join/TestAsofJoinOperator.java`):
   ```java
   @Test
   public void testBasicAsofJoin() {
       // Build side: quotes (symbol, time, price)
       Page buildPage = new Page(
           createStringsBlock("AAPL", "AAPL", "AAPL"),
           createLongsBlock(100, 200, 300), // timestamps
           createDoublesBlock(150.0, 151.0, 152.0)); // prices
       
       // Probe side: trades (symbol, time)
       Page probePage = new Page(
           createStringsBlock("AAPL", "AAPL"),
           createLongsBlock(250, 350));
       
       // Expected: trades matched with quotes at time 200 and 300
       List<Page> output = runAsofJoin(buildPage, probePage);
       
       assertEquals(output.get(0).getPositionCount(), 2);
       // Verify correct prices matched
   }
   ```

### Success Criteria
- Operator correctly performs binary search for temporal matching
- Handles NULL values appropriately
- Left/right outer joins produce correct results
- Performance is significantly better than window function approach
- All operator tests pass

---

## Agent Task 5: Memory Management and Spilling

### Context
ASOF joins can consume significant memory when build-side data is large. The operator needs to integrate with Trino's memory management and support spilling to disk when memory limits are exceeded.

### Dependencies
- Agent Task 4 (Operator Implementation) must be complete

### Your Task
Add memory accounting and spilling support to the ASOF join operator.

### Implementation Steps

1. **Create AsofJoinBridge** (`trino-main/src/main/java/io/trino/operator/join/AsofJoinBridge.java`):
   ```java
   public class AsofJoinBridge implements JoinBridge {
       private final Supplier<AsofLookupSource> lookupSourceSupplier = new Supplier<>() {
           @Override
           public AsofLookupSource get() {
               return asofLookupSourceFuture.join();
           }
       };
       
       @Override
       public long getRetainedSizeInBytes() {
           if (asofLookupSourceFuture.isDone()) {
               return asofLookupSourceFuture.join().getRetainedSizeInBytes();
           }
           return 0;
       }
       
       // Implement spilling coordination
       public ListenableFuture<Void> startSpilling() {
           return lookupSourceFactory.startSpilling();
       }
   }
   ```

2. **Create SpillableAsofLookupSource** (`trino-main/src/main/java/io/trino/operator/join/SpillableAsofLookupSource.java`):
   ```java
   public class SpillableAsofLookupSource implements AsofLookupSource {
       private final Map<Integer, SortedPartition> inMemoryPartitions;
       private final Map<Integer, SpilledPartition> spilledPartitions;
       private final SpillerFactory spillerFactory;
       private final MemoryTrackingContext memoryTrackingContext;
       
       @Override
       public SortedPartition getPartition(long hashValue) {
           int partition = hashToPartition(hashValue);
           
           // Check in-memory first
           SortedPartition memoryPartition = inMemoryPartitions.get(partition);
           if (memoryPartition != null) {
               return memoryPartition;
           }
           
           // Load from disk if spilled
           SpilledPartition spilledPartition = spilledPartitions.get(partition);
           if (spilledPartition != null) {
               return spilledPartition.load();
           }
           
           return null;
       }
       
       public void spill(SpillReason reason) {
           // Select partitions to spill based on size
           List<Integer> partitionsToSpill = selectPartitionsToSpill();
           
           for (int partition : partitionsToSpill) {
               SortedPartition data = inMemoryPartitions.remove(partition);
               SpilledPartition spilled = spillPartition(data);
               spilledPartitions.put(partition, spilled);
           }
       }
   }
   ```

3. **Update AsofLookupSourceFactory** (`trino-main/src/main/java/io/trino/operator/join/AsofLookupSourceFactory.java`):
   ```java
   public class AsofLookupSourceFactory implements LookupSourceFactory {
       private final List<List<Page>> partitionedPages;
       private final SpillerFactory spillerFactory;
       private final ListenableFuture<Void> spillInProgress = SettableFuture.create();
       
       public void sortPartitions() {
           for (int partition = 0; partition < partitionedPages.size(); partition++) {
               List<Page> pages = partitionedPages.get(partition);
               if (!pages.isEmpty()) {
                   // Sort pages by timestamp channel
                   sortPagesInPlace(pages, timeChannel);
               }
           }
       }
       
       private void sortPagesInPlace(List<Page> pages, int sortChannel) {
           // Create position mapping for sorting
           int totalPositions = pages.stream()
               .mapToInt(Page::getPositionCount)
               .sum();
           
           long[] timestamps = new long[totalPositions];
           int[] pageIndex = new int[totalPositions];
           int[] positionIndex = new int[totalPositions];
           
           // Extract timestamps and track original positions
           int offset = 0;
           for (int i = 0; i < pages.size(); i++) {
               Page page = pages.get(i);
               for (int pos = 0; pos < page.getPositionCount(); pos++) {
                   timestamps[offset] = page.getLong(sortChannel, pos);
                   pageIndex[offset] = i;
                   positionIndex[offset] = pos;
                   offset++;
               }
           }
           
           // Sort indices by timestamp
           IntArrays.quickSort(0, totalPositions, 
               (a, b) -> Long.compare(timestamps[a], timestamps[b]));
           
           // Build new sorted pages
           List<Page> sortedPages = buildSortedPages(
               pages, pageIndex, positionIndex);
           
           // Replace with sorted pages
           pages.clear();
           pages.addAll(sortedPages);
       }
   }
   ```

4. **Add Memory Context** (`trino-main/src/main/java/io/trino/operator/join/AsofJoinMemoryContext.java`):
   ```java
   public class AsofJoinMemoryContext {
       private final MemoryTrackingContext operatorMemoryContext;
       private final AtomicLong peakMemoryUsage = new AtomicLong();
       private final AtomicLong spilledBytes = new AtomicLong();
       
       public boolean tryReserveMemory(long bytes) {
           if (operatorMemoryContext.trySetBytes(
                   operatorMemoryContext.getBytes() + bytes)) {
               peakMemoryUsage.updateAndGet(current -> 
                   Math.max(current, operatorMemoryContext.getBytes()));
               return true;
           }
           return false;
       }
       
       public void releaseMemory(long bytes) {
           operatorMemoryContext.setBytes(
               operatorMemoryContext.getBytes() - bytes);
       }
   }
   ```

5. **Add Spilling Tests** (`trino-main/src/test/java/io/trino/operator/join/TestAsofJoinSpilling.java`):
   ```java
   @Test
   public void testSpillLargePartition() {
       // Create operator with limited memory
       AsofJoinOperator operator = createOperatorWithMemoryLimit(DataSize.of(1, MEGABYTE));
       
       // Add large build side that exceeds memory
       for (int i = 0; i < 1_000_000; i++) {
           operator.addInput(createQuotePage("AAPL", i, 150.0 + i * 0.01));
       }
       
       // Verify spilling occurred
       assertTrue(operator.getStats().getSpilledBytes() > 0);
       
       // Verify correctness after spilling
       Page result = operator.getOutput();
       assertNotNull(result);
   }
   ```

### Success Criteria
- Memory usage is accurately tracked
- Operator spills to disk when memory limit exceeded
- Spilled partitions can be read back correctly
- Performance degradation with spilling is acceptable
- No memory leaks under spilling scenarios

---

## Agent Task 6: Query Optimization Rules

### Context
ASOF joins can benefit from various optimizations like predicate pushdown, join reordering, and recognizing pre-sorted data. These optimizations can significantly improve query performance.

### Dependencies
- Agent Task 3 (Plan Node) must be complete

### Your Task
Implement optimizer rules specific to ASOF joins.

### Implementation Steps

1. **Create AsofJoinPushdownRule** (`trino-main/src/main/java/io/trino/sql/planner/iterative/rule/AsofJoinPushdownRule.java`):
   ```java
   public class AsofJoinPushdownRule implements Rule<AsofJoinNode> {
       @Override
       public Pattern<AsofJoinNode> getPattern() {
           return asofJoin();
       }
       
       @Override
       public Result apply(AsofJoinNode node, Captures captures, Context context) {
           // Push down filters that only reference build side
           List<Expression> buildFilters = extractBuildSideFilters(node);
           
           if (!buildFilters.isEmpty()) {
               PlanNode newBuildSource = new FilterNode(
                   context.getIdAllocator().getNextId(),
                   node.getRight(),
                   combineConjuncts(buildFilters));
               
               return Result.ofPlanNode(
                   node.withBuildSource(newBuildSource));
           }
           
           return Result.empty();
       }
   }
   ```

2. **Create AsofJoinLocalExecutionRule** (`trino-main/src/main/java/io/trino/sql/planner/iterative/rule/AsofJoinLocalExecutionRule.java`):
   ```java
   public class AsofJoinLocalExecutionRule implements Rule<AsofJoinNode> {
       @Override
       public Result apply(AsofJoinNode node, Captures captures, Context context) {
           // Check if build side is already sorted
           if (isSortedByTime(node.getRight(), node.getRightTimeSymbol())) {
               // Add property to avoid re-sorting
               return Result.ofPlanNode(
                   node.withProperty(PRESORTED_BUILD, true));
           }
           
           // Check if we should broadcast or partition
           if (shouldBroadcast(node, context)) {
               return Result.ofPlanNode(
                   node.withDistributionType(BROADCAST));
           }
           
           return Result.empty();
       }
       
       private boolean isSortedByTime(PlanNode node, Symbol timeSymbol) {
           // Check statistics and table properties
           return context.getStatsProvider()
               .getStats(node)
               .getSymbolStatistics(timeSymbol)
               .getDistinctValuesCount() > 0.9 * 
               context.getStatsProvider().getStats(node).getOutputRowCount();
       }
   }
   ```

3. **Add to PlanOptimizers** (`trino-main/src/main/java/io/trino/sql/planner/optimizations/PlanOptimizers.java`):
   ```java
   public PlanOptimizers(/* ... */) {
       // Add ASOF join rules to appropriate optimization phases
       
       ImmutableList.Builder<PlanOptimizer> optimizers = ImmutableList.builder();
       
       // ... existing optimizers ...
       
       // Add after join reordering
       optimizers.add(new IterativeOptimizer(
           "ASOF join optimizations",
           ruleStats,
           ImmutableSet.of(
               new AsofJoinPushdownRule(),
               new AsofJoinLocalExecutionRule())));
   }
   ```

4. **Create AsofJoinStatsCalculator** (`trino-main/src/main/java/io/trino/cost/AsofJoinStatsCalculator.java`):
   ```java
   public class AsofJoinStatsCalculator {
       public static PlanNodeStatsEstimate estimateAsofJoin(
               StatsProvider statsProvider,
               AsofJoinNode node) {
           
           PlanNodeStatsEstimate probeStats = statsProvider.getStats(node.getLeft());
           PlanNodeStatsEstimate buildStats = statsProvider.getStats(node.getRight());
           
           // ASOF join produces at most one match per probe row
           double outputRowCount = probeStats.getOutputRowCount();
           
           if (node.isLeftJoin()) {
               // All probe rows are output
               outputRowCount = probeStats.getOutputRowCount();
           } else {
               // Inner join: some probe rows may not match
               double temporalSelectivity = estimateTemporalSelectivity(
                   probeStats.getSymbolStatistics(node.getLeftTimeSymbol()),
                   buildStats.getSymbolStatistics(node.getRightTimeSymbol()));
               
               outputRowCount = probeStats.getOutputRowCount() * temporalSelectivity;
           }
           
           return PlanNodeStatsEstimate.builder()
               .setOutputRowCount(outputRowCount)
               .addSymbolStatistics(/* combined statistics */)
               .build();
       }
   }
   ```

5. **Add Optimizer Tests** (`trino-main/src/test/java/io/trino/sql/planner/optimizations/TestAsofJoinOptimizations.java`):
   ```java
   @Test
   public void testPredicatePushdown() {
       // Filter on build side should be pushed down
       assertPlan("SELECT * FROM trades t ASOF JOIN quotes q " +
                 "ON t.symbol = q.symbol AND t.ts >= q.ts " +
                 "WHERE q.exchange = 'NYSE'",
           output(
               asofJoin(
                   tableScan("trades"),
                   filter("exchange = 'NYSE'",
                       tableScan("quotes")))));
   }
   
   @Test  
   public void testRecognizePresortedData() {
       // If quotes are already sorted by time, avoid re-sorting
       Session session = Session.builder(getSession())
           .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "false")
           .build();
       
       assertPlan(session,
           "SELECT * FROM trades t ASOF JOIN sorted_quotes q " +
           "ON t.symbol = q.symbol AND t.ts >= q.ts",
           output(
               asofJoin(
                   tableScan("trades"),
                   tableScan("sorted_quotes")) // No sort node
                   .withProperty(PRESORTED_BUILD, true)));
   }
   ```

### Success Criteria
- Predicates are pushed through ASOF joins appropriately
- Pre-sorted data is recognized to avoid unnecessary sorting
- Join distribution type is chosen optimally
- Statistics estimation is accurate for ASOF joins
- Optimization rules improve query performance

---

## Agent Task 7: Comprehensive Testing Suite

### Context
ASOF joins need thorough testing to ensure correctness, performance, and compatibility with other Trino features.

### Dependencies
- All previous tasks should be complete

### Your Task
Create a comprehensive test suite covering unit tests, integration tests, and performance benchmarks.

### Implementation Steps

1. **Create Integration Tests** (`trino-tests/src/test/java/io/trino/tests/TestAsofJoinQueries.java`):
   ```java
   public class TestAsofJoinQueries extends AbstractTestQueryFramework {
       @Override
       protected QueryRunner createQueryRunner() throws Exception {
           return LocalQueryRunner.builder(testSessionBuilder().build())
               .withPlugin(new TpchPlugin())
               .withPlugin(new MemoryPlugin())
               .build();
       }
       
       @Test
       public void testBasicAsofJoin() {
           // Create test data
           assertUpdate("CREATE TABLE trades (symbol VARCHAR, ts BIGINT, quantity INT)");
           assertUpdate("INSERT INTO trades VALUES " +
               "('AAPL', 100, 10), ('AAPL', 300, 20), ('GOOG', 200, 30)");
           
           assertUpdate("CREATE TABLE quotes (symbol VARCHAR, ts BIGINT, price DOUBLE)");
           assertUpdate("INSERT INTO quotes VALUES " +
               "('AAPL', 50, 150.0), ('AAPL', 200, 151.0), " +
               "('AAPL', 400, 152.0), ('GOOG', 100, 2000.0)");
           
           // Test basic ASOF join
           assertQuery(
               "SELECT t.*, q.price FROM trades t " +
               "ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts",
               "VALUES ('AAPL', 100, 10, 150.0), " +
               "('AAPL', 300, 20, 151.0), " +
               "('GOOG', 200, 30, 2000.0)");
       }
       
       @Test
       public void testAsofLeftJoin() {
           // Test with unmatched rows
           assertQuery(
               "SELECT t.*, q.price FROM trades t " +
               "ASOF LEFT JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts " +
               "WHERE t.symbol = 'MSFT'",
               "VALUES ('MSFT', 150, 25, NULL)");
       }
       
       @Test
       public void testAsofJoinWithNulls() {
           // Test NULL handling in temporal column
           assertUpdate("INSERT INTO trades VALUES ('AAPL', NULL, 15)");
           
           assertQuery(
               "SELECT count(*) FROM trades t " +
               "ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts " +
               "WHERE t.ts IS NULL",
               "VALUES (0)");
       }
   }
   ```

2. **Create Correctness Tests** (`trino-tests/src/test/java/io/trino/tests/TestAsofJoinCorrectness.java`):
   ```java
   @Test
   public void testAsofVsWindowFunction() {
       // Compare ASOF JOIN with equivalent window function
       String asofQuery = 
           "SELECT t.trade_id, t.symbol, t.ts as trade_time, " +
           "q.price as quote_price, q.ts as quote_time " +
           "FROM trades t " +
           "ASOF LEFT JOIN quotes q " +
           "ON t.symbol = q.symbol AND t.ts >= q.ts";
       
       String windowQuery = 
           "WITH matched AS (" +
           "  SELECT t.trade_id, t.symbol, t.ts as trade_time, " +
           "         q.price as quote_price, q.ts as quote_time, " +
           "         ROW_NUMBER() OVER (PARTITION BY t.trade_id " +
           "                            ORDER BY q.ts DESC) as rn " +
           "  FROM trades t " +
           "  LEFT JOIN quotes q " +
           "  ON t.symbol = q.symbol AND t.ts >= q.ts" +
           ") SELECT trade_id, symbol, trade_time, quote_price, quote_time " +
           "FROM matched WHERE rn = 1 OR quote_time IS NULL";
       
       assertQuery(asofQuery, windowQuery);
   }
   
   @Test
   public void testMultipleEqualityConditions() {
       // Test ASOF join with multiple equality conditions
       assertUpdate("CREATE TABLE orders (symbol VARCHAR, exchange VARCHAR, ts BIGINT, size INT)");
       assertUpdate("CREATE TABLE market_data (symbol VARCHAR, exchange VARCHAR, ts BIGINT, bid DOUBLE, ask DOUBLE)");
       
       assertQuery(
           "SELECT o.*, m.bid, m.ask FROM orders o " +
           "ASOF JOIN market_data m " +
           "ON o.symbol = m.symbol AND o.exchange = m.exchange AND o.ts >= m.ts",
           "VALUES (...)");
   }
   ```

3. **Create Performance Benchmarks** (`trino-benchto-benchmarks/src/main/resources/benchmarks/asof-join.yaml`):
   ```yaml
   datasource: presto
   query-names: 
     - asof-join-small
     - asof-join-large
     - asof-join-vs-window
   runs: 10
   prewarm-runs: 2
   
   variables:
     scale_factor: [1, 10, 100]
   
   queries:
     asof-join-small: |
       SELECT COUNT(*) FROM (
         SELECT t.*, q.price 
         FROM trades_${scale_factor} t
         ASOF JOIN quotes_${scale_factor} q 
         ON t.symbol = q.symbol AND t.ts >= q.ts
       )
     
     asof-join-large: |
       SELECT t.*, q.* 
       FROM trades_${scale_factor} t
       ASOF JOIN quotes_${scale_factor} q 
       ON t.symbol = q.symbol AND t.ts >= q.ts
       WHERE t.trade_date = DATE '2023-01-01'
     
     asof-join-vs-window: |
       -- Equivalent window function query for comparison
       WITH matched AS (
         SELECT t.*, q.*, 
           ROW_NUMBER() OVER (PARTITION BY t.trade_id ORDER BY q.ts DESC) as rn
         FROM trades_${scale_factor} t 
         LEFT JOIN quotes_${scale_factor} q 
         ON t.symbol = q.symbol AND t.ts >= q.ts
       )
       SELECT * FROM matched WHERE rn = 1
   ```

4. **Create Distributed Tests** (`trino-tests/src/test/java/io/trino/tests/TestAsofJoinDistributed.java`):
   ```java
   public class TestAsofJoinDistributed extends AbstractTestDistributedQueries {
       @Test
       public void testDistributedAsofJoin() {
           // Test with data distributed across multiple nodes
           String createTable = "CREATE TABLE distributed_trades " +
               "WITH (partitioned_by = ARRAY['trade_date']) AS " +
               "SELECT * FROM tpch.tiny.trades";
           
           getQueryRunner().execute(createTable);
           
           // Verify ASOF join works across partitions
           assertQuery(
               "SELECT COUNT(DISTINCT t.trade_date) " +
               "FROM distributed_trades t " +
               "ASOF JOIN distributed_quotes q " +
               "ON t.symbol = q.symbol AND t.ts >= q.ts",
               "VALUES (365)"); // One year of data
       }
       
       @Test
       public void testSkewedDataDistribution() {
           // Test with highly skewed symbol distribution
           // 90% of trades are for one symbol
           assertUpdate("INSERT INTO trades " +
               "SELECT 'HOT_SYMBOL', ts, RAND() * 100 " +
               "FROM UNNEST(SEQUENCE(1, 900000)) AS t(ts)");
           
           assertUpdate("INSERT INTO trades " +
               "SELECT 'SYMBOL_' || (ts % 100), ts, RAND() * 100 " +
               "FROM UNNEST(SEQUENCE(1, 100000)) AS t(ts)");
           
           // Should still execute efficiently
           assertQuerySucceeds(
               "SELECT COUNT(*) FROM trades t " +
               "ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts");
       }
   }
   ```

5. **Create Edge Case Tests** (`trino-tests/src/test/java/io/trino/tests/TestAsofJoinEdgeCases.java`):
   ```java
   @Test
   public void testEmptyBuildSide() {
       assertQuery(
           "SELECT * FROM trades t ASOF LEFT JOIN empty_quotes q " +
           "ON t.symbol = q.symbol AND t.ts >= q.ts",
           "SELECT t.*, NULL, NULL FROM trades t");
   }
   
   @Test
   public void testDuplicateTimestamps() {
       // Multiple quotes with same timestamp
       assertUpdate("INSERT INTO quotes VALUES " +
           "('AAPL', 100, 150.0), ('AAPL', 100, 150.5)");
       
       // Should match the last one (stable sort)
       assertQuery(
           "SELECT q.price FROM trades t " +
           "ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts " +
           "WHERE t.ts = 100",
           "VALUES (150.5)");
   }
   
   @Test
   public void testVeryLargeTimestamps() {
       // Test with maximum BIGINT values
       assertUpdate("INSERT INTO trades VALUES ('X', " + Long.MAX_VALUE + ", 1)");
       assertUpdate("INSERT INTO quotes VALUES ('X', " + (Long.MAX_VALUE - 1) + ", 999.9)");
       
       assertQuery(
           "SELECT q.price FROM trades t " +
           "ASOF JOIN quotes q ON t.symbol = q.symbol AND t.ts >= q.ts " +
           "WHERE t.symbol = 'X'",
           "VALUES (999.9)");
   }
   ```

### Success Criteria
- All test cases pass consistently
- Performance benchmarks show 10x improvement over window functions
- Distributed execution handles data skew gracefully
- Edge cases are handled correctly
- No regressions in existing join functionality

---

## Agent Task 8: Documentation and Examples

### Context
Good documentation is crucial for adoption. Users need clear explanations of ASOF join semantics, syntax, and best practices.

### Dependencies
- Basic implementation should be complete

### Your Task
Create comprehensive documentation for ASOF joins in Trino.

### Implementation Steps

1. **Update SQL Reference** (`docs/src/main/sphinx/sql/select.rst`):
   ```rst
   ASOF JOIN
   ^^^^^^^^^
   
   An ``ASOF JOIN`` matches each row from the left table with the most recent row 
   from the right table based on a timestamp or sequential key. This is particularly 
   useful for time-series data where you need point-in-time lookups.
   
   Synopsis::
   
       SELECT * FROM left_table
       ASOF [LEFT | RIGHT] JOIN right_table
       ON equality_condition AND temporal_condition
   
   The temporal condition must use either ``>=`` or ``>`` operator with the left 
   side timestamp being greater than or equal to the right side timestamp.
   
   Examples
   """"""""
   
   Match trades with prevailing quotes::
   
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
   
   Match orders with account balances at order time::
   
       SELECT 
           o.order_id,
           o.account_id,
           o.amount,
           o.order_time,
           b.balance,
           b.balance_time
       FROM orders o
       ASOF LEFT JOIN account_balances b
       ON o.account_id = b.account_id
       AND o.order_time >= b.balance_time
   
   Semantics
   """""""""
   
   - Each left row matches with at most one right row
   - The match is the right row with the largest timestamp that satisfies the temporal condition
   - If no match exists, the result depends on the join type:
     - ``ASOF JOIN`` (inner): Row is filtered out
     - ``ASOF LEFT JOIN``: Right columns are NULL
     - ``ASOF RIGHT JOIN``: Left columns are NULL
   
   Performance Considerations
   """"""""""""""""""""""""""
   
   - The right (build) side is sorted by timestamp for each equality key
   - Best performance when the right side fits in memory
   - Consider partitioning large tables by equality keys
   - Pre-sorted right tables can improve performance
   ```

2. **Create Functions and Operators Guide** (`docs/src/main/sphinx/functions/temporal.rst`):
   ```rst
   Temporal Joins
   ==============
   
   ASOF JOIN vs Window Functions
   -----------------------------
   
   ASOF joins provide better performance than equivalent window function queries 
   for point-in-time lookups.
   
   **ASOF JOIN approach** (recommended)::
   
       SELECT t.*, q.price
       FROM trades t
       ASOF JOIN quotes q 
       ON t.symbol = q.symbol 
       AND t.timestamp >= q.timestamp
   
   **Window function approach** (slower)::
   
       WITH matched AS (
           SELECT 
               t.*,
               q.price,
               ROW_NUMBER() OVER (
                   PARTITION BY t.trade_id 
                   ORDER BY q.timestamp DESC
               ) as rn
           FROM trades t
           LEFT JOIN quotes q
           ON t.symbol = q.symbol 
           AND t.timestamp >= q.timestamp
       )
       SELECT * FROM matched 
       WHERE rn = 1 OR price IS NULL
   
   The ASOF JOIN is typically 10-100x faster for large datasets.
   ```

3. **Create Migration Guide** (`docs/src/main/sphinx/migration/asof-join.rst`):
   ```rst
   Migrating to ASOF JOIN
   ======================
   
   From Window Functions
   ---------------------
   
   If you're using window functions for point-in-time joins, you can migrate 
   to ASOF JOIN for better performance.
   
   **Before**::
   
       WITH latest_quotes AS (
           SELECT 
               t.trade_id,
               q.symbol,
               q.price,
               q.timestamp,
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
   
   **After**::
   
       SELECT t.trade_id, q.symbol, q.price, q.timestamp
       FROM trades t
       ASOF JOIN quotes q
       ON t.symbol = q.symbol
       AND t.timestamp >= q.timestamp
   
   From Correlated Subqueries
   --------------------------
   
   **Before**::
   
       SELECT 
           t.*,
           (SELECT price 
            FROM quotes q 
            WHERE q.symbol = t.symbol 
            AND q.timestamp <= t.timestamp
            ORDER BY q.timestamp DESC
            LIMIT 1) as quote_price
       FROM trades t
   
   **After**::
   
       SELECT t.*, q.price as quote_price
       FROM trades t
       ASOF LEFT JOIN quotes q
       ON t.symbol = q.symbol
       AND t.timestamp >= q.timestamp
   ```

4. **Create Best Practices Guide** (`docs/src/main/sphinx/admin/asof-join-tuning.rst`):
   ```rst
   ASOF JOIN Performance Tuning
   ============================
   
   Data Preparation
   ----------------
   
   1. **Pre-sort build tables**: If your right-side table is pre-sorted by 
      timestamp within each partition key, Trino can skip the sorting step::
   
          CREATE TABLE sorted_quotes AS
          SELECT * FROM quotes
          ORDER BY symbol, timestamp
   
   2. **Partition by equality keys**: For large tables, partition by the 
      equality join columns::
   
          CREATE TABLE quotes 
          WITH (partitioned_by = ARRAY['symbol'])
          AS SELECT * FROM raw_quotes
   
   Memory Configuration
   --------------------
   
   ASOF joins load the entire right side into memory for each symbol. Configure 
   adequate memory::
   
       -- Session level
       SET SESSION query_max_memory = '50GB';
       SET SESSION query_max_memory_per_node = '10GB';
       
       -- System level (config.properties)
       query.max-memory=200GB
       query.max-memory-per-node=50GB
   
   Query Optimization
   ------------------
   
   1. **Filter early**: Apply filters before the join when possible::
   
          -- Good: Filter reduces build side
          SELECT * FROM trades t
          ASOF JOIN (
              SELECT * FROM quotes WHERE exchange = 'NYSE'
          ) q ON t.symbol = q.symbol AND t.ts >= q.ts
          
          -- Less efficient: Filter after join
          SELECT * FROM trades t
          ASOF JOIN quotes q 
          ON t.symbol = q.symbol AND t.ts >= q.ts
          WHERE q.exchange = 'NYSE'
   
   2. **Use appropriate temporal operators**: Use ``>`` instead of ``>=`` when 
      you don't want exact timestamp matches
   
   Monitoring
   ----------
   
   Monitor ASOF join performance using EXPLAIN ANALYZE::
   
       EXPLAIN ANALYZE
       SELECT * FROM trades t
       ASOF JOIN quotes q
       ON t.symbol = q.symbol AND t.ts >= q.ts
   
   Key metrics to watch:
   - Build side row count and memory usage
   - Number of partitions
   - Sort time (if not pre-sorted)
   - Probe time per row
   ```

5. **Create Example Queries** (`docs/src/main/sphinx/sql/asof-join-examples.rst`):
   ```rst
   ASOF JOIN Examples
   ==================
   
   Financial Market Data
   ---------------------
   
   **Trade-Quote Matching**::
   
       -- Match each trade with the prevailing bid/ask
       SELECT 
           t.trade_id,
           t.symbol,
           t.price as trade_price,
           t.quantity,
           t.timestamp as trade_time,
           q.bid,
           q.ask,
           q.timestamp as quote_time,
           t.price - q.bid as bid_diff,
           q.ask - t.price as ask_diff
       FROM trades t
       ASOF JOIN quotes q
       ON t.symbol = q.symbol 
       AND t.timestamp >= q.timestamp
       WHERE t.trade_date = CURRENT_DATE
   
   **Order Book Reconstruction**::
   
       -- Get order book state at each trade
       SELECT 
           t.trade_id,
           t.symbol,
           t.timestamp,
           ob.bid_price_1,
           ob.bid_size_1,
           ob.ask_price_1,
           ob.ask_size_1
       FROM trades t
       ASOF JOIN order_book_snapshots ob
       ON t.symbol = ob.symbol
       AND t.timestamp >= ob.timestamp
   
   IoT and Sensor Data
   -------------------
   
   **Sensor Reading Interpolation**::
   
       -- Match events with most recent sensor reading
       SELECT 
           e.event_id,
           e.device_id,
           e.event_type,
           e.timestamp as event_time,
           s.temperature,
           s.humidity,
           s.timestamp as reading_time,
           e.timestamp - s.timestamp as reading_age_seconds
       FROM device_events e
       ASOF LEFT JOIN sensor_readings s
       ON e.device_id = s.device_id
       AND e.timestamp >= s.timestamp
       WHERE e.timestamp - s.timestamp < INTERVAL '5' MINUTE
   
   Business Analytics
   ------------------
   
   **Customer Status at Transaction Time**::
   
       -- Get customer tier at time of each transaction
       SELECT 
           t.transaction_id,
           t.customer_id,
           t.amount,
           t.timestamp,
           cs.tier,
           cs.credit_limit,
           cs.effective_date
       FROM transactions t
       ASOF JOIN customer_status cs
       ON t.customer_id = cs.customer_id
       AND t.timestamp >= cs.effective_date
   
   **Inventory Levels at Order Time**::
   
       -- Check inventory when order was placed
       SELECT 
           o.order_id,
           o.product_id,
           o.quantity as ordered_quantity,
           o.order_time,
           i.quantity_on_hand,
           i.last_updated,
           CASE 
               WHEN i.quantity_on_hand >= o.quantity THEN 'In Stock'
               ELSE 'Back Order'
           END as fulfillment_status
       FROM orders o
       ASOF LEFT JOIN inventory_snapshots i
       ON o.product_id = i.product_id
       AND o.order_time >= i.last_updated
   ```

### Success Criteria
- Documentation clearly explains ASOF join concepts
- Examples cover common use cases
- Performance tuning guide helps users optimize queries
- Migration guide helps users convert existing queries
- Documentation is integrated into Trino's docs build

---

## Implementation Order and Dependencies

1. **Phase 1 - Core Implementation**
   - Agent Task 1: Parser Support (no dependencies)
   - Agent Task 2: Analyzer Support (depends on Task 1)
   - Agent Task 3: Plan Node (depends on Task 2)

2. **Phase 2 - Execution**
   - Agent Task 4: Operator Implementation (depends on Task 3)
   - Agent Task 5: Memory Management (depends on Task 4)

3. **Phase 3 - Optimization & Polish**
   - Agent Task 6: Query Optimization (depends on Task 3)
   - Agent Task 7: Testing Suite (depends on all implementation tasks)
   - Agent Task 8: Documentation (can start early, finalize after implementation)

## Notes for Agents

- Each task includes all necessary context about ASOF joins
- Code locations are specific to help you navigate the codebase
- Example implementations are provided as guidance
- Test cases are included to verify correctness
- Success criteria are clear and measurable
- Dependencies are explicitly stated