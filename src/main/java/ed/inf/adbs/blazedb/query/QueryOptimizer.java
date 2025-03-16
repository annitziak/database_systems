package ed.inf.adbs.blazedb.query;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;

public class QueryOptimizer {

    public static boolean canApplyEarlyProjection(PlainSelect plainSelect) {
        // Check if SELECT * is used -> early projection is NOT possible
        List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
        if (selectItems.get(0).toString().equals("*")) {
            return false;
        }

        // Check if there are any aggregate functions in SELECT clause
        // If there are and they concern a column then early projection is NOT possible because
        // the aggregate functions will be computed later
        // but if they concern a constant then early projection is possible with any column
        // here also collect the tables that are used in the query
        Set<String> tables = new HashSet<>();
        for (SelectItem<?> item : selectItems) {
            if (item.toString().toUpperCase().startsWith("SUM(")) {
                return false;
            }else{
                tables.add(item.toString().split("\\.")[0]);
            }
        }
        System.out.println("Tables: " + tables);
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

    // ✅ Extract columns from WHERE, ORDER BY, GROUP BY, HAVING
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
    private static Set<String> getReferencedTables(Expression expr) {
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

}
