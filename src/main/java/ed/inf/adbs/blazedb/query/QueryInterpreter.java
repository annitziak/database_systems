package ed.inf.adbs.blazedb.query;

import ed.inf.adbs.blazedb.dbcatalogue.DBCatalogue;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static ed.inf.adbs.blazedb.query.QueryOptimizer.belongsToTable;

/**
 * QueryInterpreter interprets the SQL query and generates a query plan.
 */
public class QueryInterpreter {

    private final DBCatalogue dbCatalogue;

    public QueryInterpreter(DBCatalogue dbCatalogue) {
        this.dbCatalogue = dbCatalogue;
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
        rootOperator = new ScanOperator(plainSelect.getFromItem(), dbCatalogue);

        // Attempt to do early projection if possible
        boolean earlyProjection = QueryOptimizer.canApplyEarlyProjection(plainSelect);
        List<SelectItem<?>> selectItems = plainSelect.getSelectItems();

        // we only project the columns that are needed from the base table here
        if (earlyProjection) {
            System.out.println("[EARLY PROJECTION] with statement " + selectItems);
            rootOperator = new ProjectOperator(rootOperator, selectItems);}

        // Handle WHERE
        // we need to filter out all the tuples from the fromItem table
        // that satisfy these conditions. First collect all the conditions that satisfy this
        // and send them to the SelectOperator. The rest of them should still be saved
        // they will be applied after the scan of the next table in the join block
        // Handle WHERE
        // Handle WHERE: Extract and separate conditions

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
        List<Join> joins = plainSelect.getJoins();
        if (joins != null) {
            for (Join join : joins) {
                // Scan the right table
                Operator rightTable = new ScanOperator(join.getRightItem(), dbCatalogue);

                // Extract conditions that ONLY belong to the right table
                List<Expression> rightTableConditions = new ArrayList<>();
                List<Expression> remainingJoinConditions = new ArrayList<>();

                for (Expression condition : joinConditions) {
                    if (QueryOptimizer.isSingleTableCondition(condition, join.getRightItem())) {  // ✅ Only push if single-table
                        rightTableConditions.add(condition);
                    } else {
                        remainingJoinConditions.add(condition);  // Multi-table conditions stay for join
                    }
                }

                // Apply selection pushdown to the right table if applicable
                if (!rightTableConditions.isEmpty()) {
                    Expression pushedDownCondition = QueryOptimizer.mergeConditions(rightTableConditions);
                    System.out.println("[PUSH DOWN] with statement IN RIGHT " + pushedDownCondition);
                    rightTable = new SelectOperator(rightTable, pushedDownCondition);
                }

                // Update join conditions to exclude pushed-down ones
                System.out.println("Remaining join conditions: " + remainingJoinConditions);
                Expression joinConditionExpr = QueryOptimizer.mergeConditions(remainingJoinConditions);

                // Perform the join using only the remaining multi-table conditions
                rootOperator = new JoinOperator(rootOperator, rightTable, joinConditionExpr);
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