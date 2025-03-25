package ed.inf.adbs.blazedb.query;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;
import ed.inf.adbs.blazedb.dbcatalogue.DBStatistics;

public class QueryOptimizer {

    public static boolean canApplyEarlyProjection(PlainSelect plainSelect) {
        // Check if SELECT * is used -> early projection is NOT possible
        List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
        if (selectItems.get(0).toString().equals("*")) {
            return false;
        }

        // Check if there are any aggregate functions in SELECT clause, if there are
        // early projection is NOT possible since they are computed at the end.

        Set<String> tables = new HashSet<>();
        for (SelectItem<?> item : selectItems) {
            if (item.toString().toUpperCase().startsWith("SUM(")) {
                return false;
            }else{
                tables.add(item.toString().split("\\.")[0]);
            }
        }

        // also if they concern more than one table, this becomes more hectic to keep track of
        // as we need to keep a per-table list of columns to project.
        // this was only done in the selection case, but here it doesn't seem to add too much value.
        if (tables.size() > 1) {
            return false;
        }

        // Extract the projected columns from SELECT clause to compare with required columns
        Set<String> selectColumns = new HashSet<>();
        for (SelectItem<?> item : selectItems) {
            selectColumns.add(item.toString());  // Convert each SelectItem to column name
        }

        // Step 3: Extract columns from WHERE, ORDER BY, GROUP BY
        Set<String> requiredColumns = new HashSet<>();

        // Extract from WHERE clause
        if (plainSelect.getWhere() != null) {
            requiredColumns.addAll(extractColumnsFromExpression(plainSelect.getWhere()));
        }

        // Extract from GROUP BY clause
        if (plainSelect.getGroupBy() != null) {
            GroupByElement groupByElement = plainSelect.getGroupBy();
            List<Column> groupByColumns = groupByElement.getGroupByExpressionList();

            // Convert each Column to String before adding to requiredColumns
            for (Column groupBy : groupByColumns) {
                requiredColumns.add(groupBy.toString());  // Convert Column to String
            }
        }

        // Extract from ORDER BY clause
        if (plainSelect.getOrderByElements() != null) {
            for (OrderByElement orderBy : plainSelect.getOrderByElements()) {
                requiredColumns.addAll(extractColumnsFromExpression(orderBy.getExpression()));
            }
        }

        System.out.println("Select columns: " + selectColumns);
        System.out.println("Required columns: " + requiredColumns);
        // Step 4: Early projection is possible only if selected columns cover required columns**
        return selectColumns.containsAll(requiredColumns);
    }

    public static List<Expression> extractConditions(Expression whereExpression) {
        List<Expression> conditions = new ArrayList<>();
        if (whereExpression instanceof AndExpression) {
            AndExpression andExpr = (AndExpression) whereExpression;
            while (andExpr.getLeftExpression() instanceof AndExpression) {
                conditions.add(andExpr.getRightExpression());
                andExpr = (AndExpression) andExpr.getLeftExpression();
            }
            conditions.add(andExpr.getLeftExpression());
            conditions.add(andExpr.getRightExpression());
        } else if (whereExpression != null) {
            conditions.add(whereExpression);
        }
        return conditions;
    }

    // Merge a list of expressions into a single AND expression
    public static Expression mergeConditions(List<Expression> conditions) {
        if (conditions.isEmpty()) {
            return null;
        }
        Expression result = conditions.get(0);
        for (int i = 1; i < conditions.size(); i++) {
            result = new AndExpression(result, conditions.get(i));
        }
        return result;
    }

    // Extract only the conditions that belong to a specific table
    public static Expression extractTableCondition(FromItem table, List<Expression> conditions) {
        List<Expression> tableConditions = new ArrayList<>();
        for (Expression condition : conditions) {
            if (belongsToTable(condition, table)) {
                tableConditions.add(condition);
            }
        }
        return mergeConditions(tableConditions);
    }

    // Extract columns from an expression
    private static Set<String> extractColumnsFromExpression(Expression expr) {
        Set<String> columns = new HashSet<>();

        if (expr instanceof BinaryExpression) {
            BinaryExpression binaryExp = (BinaryExpression) expr;
            columns.addAll(extractColumnsFromExpression(binaryExp.getLeftExpression()));
            columns.addAll(extractColumnsFromExpression(binaryExp.getRightExpression()));
        } else if (expr instanceof Column) {
            columns.add(((Column) expr).getFullyQualifiedName());
        } else if (expr instanceof Parenthesis) {
            columns.addAll(extractColumnsFromExpression(((Parenthesis) expr).getExpression()));
        } else if (expr instanceof InExpression) {
            columns.add(((InExpression) expr).getLeftExpression().toString());
        }

        return columns;
    }

    public static boolean belongsToTable(Expression condition, FromItem table) {
        // Convert table to string (handles aliases)
        String tableName = table.toString();

        // Convert condition to string and check if it mentions only this table
        return condition.toString().matches(".*\\b" + tableName + "\\.[a-zA-Z0-9_]+\\b.*");
    }


    public static boolean isSingleTableCondition(Expression condition, FromItem table) {
        Set<String> referencedTables = getReferencedTables(condition);
        return referencedTables.size() == 1 && referencedTables.contains(table.toString());
    }

    // Extract tables referenced in an expression
    public static Set<String> getReferencedTables(Expression expr) {
        Set<String> tables = new HashSet<>();

        if (expr instanceof BinaryExpression) {
            BinaryExpression binaryExpr = (BinaryExpression) expr;
            tables.addAll(getReferencedTables(binaryExpr.getLeftExpression()));
            tables.addAll(getReferencedTables(binaryExpr.getRightExpression()));
        } else if (expr instanceof Column) {
            Column column = (Column) expr;
            if (column.getTable() != null) {
                tables.add(column.getTable().getName());
            }
        }

        return tables;
    }
    // this will get join condition and return the cardinality of the join
    // different cases of =, > , < based on the statistics : ntuples, distinct values, min, max



    public static List<Join> reorderJoins(List<Join> joins, FromItem baseTable, DBStatistics dbStatistics, List<Expression> joinConditions) {
        List<Join> orderedJoins = new ArrayList<>();
        Set<String> scannedTables = new HashSet<>();
        scannedTables.add(baseTable.toString());

        List<Join> remainingJoins = new ArrayList<>(joins);

        while (!remainingJoins.isEmpty()) {
            List<Join> candidateJoins = new ArrayList<>();

            // First: filter joins that are connected to any already scanned table
            for (Join join : remainingJoins) {
                String rightTable = join.getRightItem().toString();

                for (Expression condition : joinConditions) {
                    Set<String> tablesInCond = getReferencedTables(condition);
                    if (tablesInCond.contains(rightTable) && intersects(scannedTables, tablesInCond)) {
                        candidateJoins.add(join);
                        break;
                    }
                }
            }

            Join bestJoin = null;

            if (candidateJoins.size() == 1) {
                bestJoin = candidateJoins.get(0);
            } else if (candidateJoins.size() > 1) {
                // Multiple join options connected to scanned tables — choose lowest selectivity
                double bestSelectivity = Double.MAX_VALUE;

                for (Join join : candidateJoins) {
                    String rightTable = join.getRightItem().toString();
                    for (Expression condition : joinConditions) {
                        Set<String> tablesInCond = getReferencedTables(condition);
                        if (tablesInCond.contains(rightTable) && scannedTables.containsAll(tablesInCond)) {
                            double sel = estimateSelectivity(condition, dbStatistics);
                            if (sel < bestSelectivity) {
                                bestSelectivity = sel;
                                bestJoin = join;
                            }
                        }
                    }
                }
            }

            if (bestJoin != null) {
                orderedJoins.add(bestJoin);
                scannedTables.add(bestJoin.getRightItem().toString());
                remainingJoins.remove(bestJoin);
            } else {
                // Fallback (no connected join — Cartesian join case)
                orderedJoins.add(remainingJoins.remove(0));
            }
        }

        System.out.println("Ordered joins: " + orderedJoins);
        return orderedJoins;
    }


    // helper to check if two sets intersect
    private static boolean intersects(Set<String> scanned, Set<String> condTables) {
        for (String t : condTables) {
            if (scanned.contains(t)) return true;
        }
        return false;
    }


    private static double estimateSelectivity(Expression joinCondition, DBStatistics dbStatistics) {
        String[] parts = joinCondition.toString().split("=");
        if (parts.length != 2) return 1.0;

        String leftPart = parts[0].trim();
        String rightPart = parts[1].trim();

        String[] leftParts = leftPart.split("\\.");
        String[] rightParts = rightPart.split("\\.");

        if (leftParts.length != 2 || rightParts.length != 2) return 1.0;

        String leftTable = leftParts[0];
        String leftColumn = leftParts[1];
        String rightTable = rightParts[0];
        String rightColumn = rightParts[1];

        List<Integer> leftStats = dbStatistics.getColumnStatistics(leftTable, leftColumn);
        List<Integer> rightStats = dbStatistics.getColumnStatistics(rightTable, rightColumn);

        if (leftStats == null || rightStats == null) return 1.0;

        int leftDistinct = leftStats.get(2);
        int rightDistinct = rightStats.get(2);

        return 1.0 / Math.max(leftDistinct, rightDistinct);
    }


}






