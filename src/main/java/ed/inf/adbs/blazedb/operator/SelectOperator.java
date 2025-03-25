package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import net.sf.jsqlparser.expression.Expression;
import ed.inf.adbs.blazedb.visitor.ExpressionVisitor;

/**
 * SelectOperator filters tuples based on a given condition.
 */

public class SelectOperator extends Operator {
    private Operator child;
    private Expression expression;

    /**
     * Constructs a SelectOperator with the specified child operator and condition.
     *
     * @param child      The child operator producing tuples.
     * @param expression The condition to filter tuples.
     */

    public SelectOperator(Operator child, Expression expression) {
        this.child = child;
        this.expression = expression; // The condition to filter tuples
    }

    /**
     * Retrieves the next tuple that satisfies the condition.
     *
     * @return The next tuple that satisfies the condition.
     */

    @Override
    public Tuple getNextTuple() {
        while (true) {
            // get the next tuple from the child operator
            Tuple nextTuple = child.getNextTuple();

            if (nextTuple == null) {
                return null; // End of data
            }

            if (expression == null) {
                return nextTuple; // No condition to check : return the tuple as is. Good practice.
            }

            // Create a visitor to evaluate the expression. The visitor will return a boolean result.
            ExpressionVisitor expressionVisitor = new ExpressionVisitor(nextTuple);
            expression.accept(expressionVisitor);

            // Check if the tuple satisfies the condition and if so, return it.
            if (expressionVisitor.getResult()) {
                return nextTuple;
            }
        }
    }

    /**
     * Resets the child operator.
     */
    @Override
    public void reset() {
        this.child.reset();
    }

}
