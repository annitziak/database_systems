## Advanced Database Systems
Author : s2704516

# Task 1
**Extracting Join Conditions** :The logic for extracting join conditions from the WHERE clause is implemented in the interpret method of QueryInterpreter.java. The key steps are:
The WHERE clause is parsed, and all conditions are retrieved using QueryOptimizer.extractConditions(whereExpression). After this the conditions are classified as: <br>
1. Base Table Conditions (Optimization : single-table filters applied early). <br>
2. Join Conditions (multi-table conditions applied later). <br>

We then recursively apply push Down Single-Table Conditions using the `SELECT` operator after we initiate a `SCAN` operator for each of the tables in the `fromItem` object, and crucially before the 
`JOIN` operation. The other multi-table conditions are applied during the `JOIN` processing.
The logic is also detailed in inline comments inside the interpret method

# Task 2 : Optimizations


# Process Description
- Parsing SQL Query <br>
The SQL query is read and parsed using JSQLParser. <br>
- Building the Query Execution Plan
**Scan Operator:** Reads the base table.
**Projection Optimization:** If applicable, unnecessary columns are eliminated early. 
**WHERE Clause Handling:** Filters are applied efficiently through selection pushdown for base table conditions.
**JOIN Handling:** Joins are processed with extracted conditions. This is the most expensive operation.
**GROUP BY and Aggregation:** Grouping and sum functions are processed. Important to note is that all cases have been handled. For example a sum can exist with and without a Group By and vice versa.
**ORDER BY Processing:** Sorting is applied. This is always ascending.
**Projection Finalization:** Ensures only necessary columns are included if early projection was not possible.
**DISTINCT Handling:** Duplicate elimination if required.
This step-by-step approach ensures an optimized and structured query execution.

# File Organization
1. Within the `src/main` : <br>
`dbcatalogue`: contains dbcatalogue class <br>
`operator` : contains the base Operator class and the seven operators supported: Select, Project, DuplicateElimination, Sum, Join, Scan, Sort <br>
`query`: contains query optimizer, plan and interpreter <br>
`visitor`: contains expression visitor to evaluate conditions <br>

2. Within the `src/test`: <br>
Tests for each operator, dbcatalogue and the tests for each query given in BlazeDBTest. The operators tests were done initially for bug debugging having as a child the scan operator.
Further testing was also done with a set of other queries to test the optimizations but also more complex and tricky queries. These queries can be found under `samples\extra_input`

2. Within `samples` <br>
All the given testing queries, the output and the expected output as well as the `extra_input` directory which contains a set of other 12 queries which were tested. These queries are more complex, for example
they contain more AND conditions, more edge case scenarios such as `SELECT DISTINCT(*) FROM Students`. It was seen that for these queries the optimizations done in Task 2 reduced the processing time
more significantly. For example for the extra query 11 x less time.

