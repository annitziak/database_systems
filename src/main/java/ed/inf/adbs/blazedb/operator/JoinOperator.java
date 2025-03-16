package ed.inf.adbs.blazedb.operator;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.visitor.ExpressionVisitor;
import net.sf.jsqlparser.expression.Expression;

public class JoinOperator extends Operator {

    private final Operator leftChild;
    private final Operator rightChild;
    private final Expression expression;
    private Tuple currentLeftTuple;

    /**
     * Create a new JoinOperator with the given left and right children and join condition.
     * @param leftChild The left child operator.
     * @param rightChild The right child operator.
     * @param expression The join condition.
     */

    public JoinOperator(Operator leftChild, Operator rightChild, Expression expression) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.expression = expression;
        this.currentLeftTuple = null; // since we are building a left-deep tree
    }

    /**
     * Evaluate the join condition for the given pair of tuples.
     * @param leftTuple The left tuple.
     * @param rightTuple The right tuple.
     * @return True if the join condition is satisfied, false otherwise.
     * This uses the ExpressionVisitor to evaluate the expression.
     */

    public boolean evaluateExpression(Tuple leftTuple, Tuple rightTuple) {
        if (expression == null) {
            // if there is no join condition, is the same as a cross join!
            return true;
        }

        // Merge tuples before passing to ExpressionVisitor
        // The expression visitor expects a single tuple with all attributes.
        Tuple mergedTuple = Tuple.merge(leftTuple, rightTuple);
        ExpressionVisitor expressionVisitor = new ExpressionVisitor(mergedTuple);
        this.expression.accept(expressionVisitor);

        //System.out.println("Debugging : Evaluating: " + leftTuple + " ⨝ " + rightTuple + " -> " + expressionVisitor.getResult());
        return expressionVisitor.getResult();
    }


    // Note to self: look at this again
    /**
     * Get the next tuple that satisfies the join condition.
     * @return The next tuple that satisfies the join condition.
     * Important here is that we are evaluating the condition by making the left tuple
     * fixed as the outer tuple and the right tuple the inner tuple which we are iterating over.
     */
    @Override
    public Tuple getNextTuple() {

        // If currentLeftTuple is null, get the next left tuple
        if (this.currentLeftTuple == null) {
            this.currentLeftTuple = this.leftChild.getNextTuple();
        }

        // If no more left tuples, return null : end of join!
        if (this.currentLeftTuple == null) {
            return null;
        }

        // If the left tuple is not null, get the next right tuple to compare for the join condition
        Tuple rightTuple = rightChild.getNextTuple();

        if (rightTuple == null) {
            // Right side is exhausted, reset and move to next left tuple
            rightChild.reset();
            this.currentLeftTuple = this.leftChild.getNextTuple();

            // If no more left tuples, return null
            if (this.currentLeftTuple == null) {
                return null;
            }

            // Recursively call getNextTuple() to get a new valid pair
            return this.getNextTuple();
        }

        // Evaluate join condition before merging
        if (evaluateExpression(this.currentLeftTuple, rightTuple)) {
            return Tuple.merge(this.currentLeftTuple, rightTuple);
        }

        // Recursively call getNextTuple() to continue searching for valid pairs
        return this.getNextTuple();
    }

    /**
     * Reset the join operator.
     * This resets the left and right children and sets the current left tuple to null.
     */

    @Override
    public void reset() {
        this.leftChild.reset();
        this.rightChild.reset();
        this.currentLeftTuple = null;
    }
}
