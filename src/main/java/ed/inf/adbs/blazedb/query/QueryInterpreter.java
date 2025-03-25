package ed.inf.adbs.blazedb.query;

import ed.inf.adbs.blazedb.dbcatalogue.DBCatalogue;
import ed.inf.adbs.blazedb.dbcatalogue.DBStatistics;
import ed.inf.adbs.blazedb.operator.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;


import net.sf.jsqlparser.expression.Function;


import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

import static ed.inf.adbs.blazedb.query.QueryOptimizer.belongsToTable;

/**
 * QueryInterpreter interprets the SQL query and generates a query plan.
 */
public class QueryInterpreter {

    private final DBCatalogue dbCatalogue;
    private final DBStatistics dbstatistics;

    public QueryInterpreter(DBCatalogue dbCatalogue) {
        this.dbCatalogue = dbCatalogue;
        this.dbstatistics = new DBStatistics(dbCatalogue);
    }

    /**
     * Interprets the SQL query and generates a query plan.
     *
     * @param QueryPath Path to the SQL query file.
     * @return QueryPlan object representing the query plan.
     * @throws FileNotFoundException If the file is not found.
     * @throws JSQLParserException  If there is an error parsing the SQL query.
     */
    public QueryPlan interpret(String QueryPath) throws FileNotFoundException, JSQLParserException {
        //parse using the JSQLParser
        Statement statement = CCJSqlParserUtil.parse(new FileReader(QueryPath));

        //Use plainSelect
        PlainSelect plainSelect = (PlainSelect) statement;

        // Each step modifies rootOperator by wrapping it inside a new operator,
        // keeping the previous operator as a child. This builds a tree-like structure.


        // MANDATORY : SCAN
        Operator rootOperator = null;
        System.out.println("From item: " + plainSelect.getFromItem());
        rootOperator = new ScanOperator(plainSelect.getFromItem(), dbCatalogue);

        // Attempt to do early projection if possible
        boolean earlyProjection = QueryOptimizer.canApplyEarlyProjection(plainSelect);
        List<SelectItem<?>> selectItems = plainSelect.getSelectItems();

        // we only project the columns that are needed from the base table here
        if (earlyProjection) {
            rootOperator = new ProjectOperator(rootOperator, selectItems);}

        // Handle WHERE
        // we need to filter out all the tuples from the fromItem table
        // that satisfy these conditions. First collect all the conditions that satisfy this
        // and send them to the SelectOperator. The rest of them should still be saved
        // they will be applied after the scan of the next table in the join block

        //define here but check
        List<Expression> joinConditions = new ArrayList<>();
        if (plainSelect.getWhere() != null) {
            Expression whereExpression = plainSelect.getWhere();
            List<Expression> allConditions = QueryOptimizer.extractConditions(whereExpression);

            // Separate conditions into:
            // 1. Conditions for the FROM table (selection pushdown)
            // 2. Conditions for JOINs (kept for later)
            List<Expression> baseTableConditions = new ArrayList<>();

            for (Expression condition : allConditions) {
                if (QueryOptimizer.isSingleTableCondition(condition, plainSelect.getFromItem())) {
                    baseTableConditions.add(condition);
                } else {
                    joinConditions.add(condition);
                }
            }

            // Apply selection pushdown if there are conditions for the base table
            if (!baseTableConditions.isEmpty()) {
                Expression pushedDownConditions = QueryOptimizer.mergeConditions(baseTableConditions);
                System.out.println("[PUSH DOWN] with statement " + pushedDownConditions);
                rootOperator = new SelectOperator(rootOperator, pushedDownConditions);
            }
        }

        System.out.println(("Join conditions: " + joinConditions));
// Handle JOIN: joinConditions is now accessible here
// if multiple join conditions are given we can make the order better
// but making sure the first condition regards the fromItem table!
        List<Join> joins = plainSelect.getJoins();
        System.out.println("Joins" + joins);
        if (joins != null) {
            // here make sure that the join is the one that regards the fromItem table
            // that is already scanned, if not swap them and do this iteratively to make sure
            // all the joins are handled
            // reorder the join conditions
            //List<Join> orderedJoins = QueryOptimizer.findJoinOrder(joins, plainSelect.getFromItem());
            // find the join that contains either on the left or right the base condition
            // and add it to the orderedJoins list first and then the next one should be the one with the
            // same table as the previous join's right table
            //System.out.println("Ordered joins: " + orderedJoins);

            List<Join> orderedJoins;
            if (joins.size() >= 2) {
                orderedJoins = QueryOptimizer.reorderJoins(joins, plainSelect.getFromItem(), dbstatistics, joinConditions);
            } else {
                orderedJoins = joins;
            }

            Set<String> joinedTables = new HashSet<>();
            joinedTables.add(plainSelect.getFromItem().toString());  // Base table

            for (Join join : orderedJoins) {
                String rightTableName = join.getRightItem().toString();
                System.out.println("Processing with join table " + rightTableName);

                Operator rightTable = new ScanOperator(join.getRightItem(), dbCatalogue);

                List<Expression> rightTableConditions = new ArrayList<>();
                List<Expression> applicableJoinConditions = new ArrayList<>();
                List<Expression> remainingJoinConditions = new ArrayList<>();

                Set<String> availableNow = new HashSet<>(joinedTables);
                availableNow.add(rightTableName);

                for (Expression condition : joinConditions) {
                    Set<String> conditionTables = QueryOptimizer.getReferencedTables(condition);

                    if (conditionTables.size() == 1 && conditionTables.contains(rightTableName)) {
                        // Right-table selection pushdown
                        rightTableConditions.add(condition);
                    } else if (availableNow.containsAll(conditionTables)) {
                        // Both sides of condition are already scanned
                        applicableJoinConditions.add(condition);
                    } else {
                        // Still waiting for a table not yet scanned
                        remainingJoinConditions.add(condition);
                    }
                }

                // Apply pushdown
                if (!rightTableConditions.isEmpty()) {
                    Expression pushedDown = QueryOptimizer.mergeConditions(rightTableConditions);
                    System.out.println("[PUSH DOWN] with statement IN RIGHT " + pushedDown);
                    rightTable = new SelectOperator(rightTable, pushedDown);
                }

                Expression joinConditionExpr = QueryOptimizer.mergeConditions(applicableJoinConditions);
                System.out.println("Join condition used now: " + joinConditionExpr);
                rootOperator = new JoinOperator(rootOperator, rightTable, joinConditionExpr);

                // Update for next iteration
                joinConditions = remainingJoinConditions;
                joinedTables.add(rightTableName);
            }

        }



        // Handle GROUP BY and SUM
        List<Function> sumFunctions = new ArrayList<>();  //Create a list to store all SUM functions

        for (SelectItem<?> selectItem : plainSelect.getSelectItems()) {
            if (selectItem.getExpression() instanceof Function) {
                sumFunctions.add((Function) selectItem.getExpression());  // Add all SUM functions
            }
        }

        GroupByElement groupByElement = plainSelect.getGroupBy();
        if (groupByElement != null || !sumFunctions.isEmpty()) {
            rootOperator = new SumOperator(rootOperator, groupByElement, sumFunctions);
        }

        // Handle ORDER BY
        if (plainSelect.getOrderByElements() != null) {

            System.out.println("[ORDER BY] with statement " + plainSelect.getOrderByElements());
            rootOperator = new SortOperator(rootOperator, plainSelect.getOrderByElements()); }


        // Apply projection at the end if SELECT * is not used and early projection is not done
        if (!plainSelect.getSelectItems().get(0).toString().equals("*") && !earlyProjection) {
            rootOperator = new ProjectOperator(rootOperator, plainSelect.getSelectItems());
        }

        // In case of DISTINCT, wrap the root operator with DuplicateEliminationOperator
        if (plainSelect.getDistinct() != null) {

            if (!selectItems.isEmpty()) {
                String distinctColumn = selectItems.get(0).toString();
                System.out.println("Distinct column: " + distinctColumn);
                rootOperator = new DuplicateEliminationOperator(rootOperator, distinctColumn);
            }else{
                System.out.println("Distinct column: " + "No column specified");
            }
        }
            // create a new QueryPlan object with the root operator
            return new QueryPlan(rootOperator);
        }
    }