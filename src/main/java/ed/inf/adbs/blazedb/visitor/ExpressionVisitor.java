package ed.inf.adbs.blazedb.visitor;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

/**
 * ExpressionVisitor for evaluating conditions on a merged Tuple.
 */
public class ExpressionVisitor extends ExpressionDeParser {
    private Tuple mergedTuple;
    private boolean result;

    /**
     * Constructor for ExpressionVisitor.
     */
    public ExpressionVisitor(Tuple mergedTuple) {
        this.mergedTuple = mergedTuple;
        this.result = false;
    }

    public boolean getResult() {
        return result;
    }

    @Override
    public void visit(AndExpression andExpression) {
        andExpression.getLeftExpression().accept(this);
        boolean left = result;
        andExpression.getRightExpression().accept(this);
        result = left && result;
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        Object leftValue = evaluate(equalsTo.getLeftExpression());
        Object rightValue = evaluate(equalsTo.getRightExpression());
        result = leftValue != null && rightValue != null && Integer.valueOf(leftValue.toString()).equals(Integer.valueOf(rightValue.toString()));
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        Object leftValue = evaluate(greaterThan.getLeftExpression());
        Object rightValue = evaluate(greaterThan.getRightExpression());
        result = compareValues(leftValue, rightValue) > 0;
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        Object leftValue = evaluate(greaterThanEquals.getLeftExpression());
        Object rightValue = evaluate(greaterThanEquals.getRightExpression());
        result = compareValues(leftValue, rightValue) >= 0;
    }

    @Override
    public void visit(MinorThan minorThan) {
        Object leftValue = evaluate(minorThan.getLeftExpression());
        Object rightValue = evaluate(minorThan.getRightExpression());
        result = compareValues(leftValue, rightValue) < 0;
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        Object leftValue = evaluate(minorThanEquals.getLeftExpression());
        Object rightValue = evaluate(minorThanEquals.getRightExpression());
        result = compareValues(leftValue, rightValue) <= 0;
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        Object leftValue = evaluate(notEqualsTo.getLeftExpression());
        Object rightValue = evaluate(notEqualsTo.getRightExpression());
        result = leftValue != null && rightValue != null && !leftValue.equals(rightValue);
    }

    /**
     * Extracts the actual column value from the merged tuple.
     */
    private Object evaluate(Expression expr) {
        if (expr instanceof Column) {
            Column column = (Column) expr;
            String columnName = column.getColumnName();
            String tableName = column.getTable() != null ? column.getTable().getName() : null;

            return mergedTuple.returnValue(columnName, tableName);
        }
        if (expr instanceof LongValue) {
            return ((LongValue) expr).getValue();
        }
        if (expr instanceof StringValue) {
            return ((StringValue) expr).getValue();
        }
        return null;
    }

    /**
     * Compares two values.
     */
    private int compareValues(Object left, Object right) {
        if (left == null || right == null) {
            return -1; // If one of them is null, the comparison fails
        }

        if (left instanceof Number && right instanceof Number) {
            return Double.compare(((Number) left).doubleValue(), ((Number) right).doubleValue());
        }

        if (left instanceof String && right instanceof String) {
            return ((String) left).compareTo((String) right);
        }

        return -1; // Incompatible types
    }
}
