package ed.inf.adbs.blazedb.query;

import ed.inf.adbs.blazedb.Tuple;
import ed.inf.adbs.blazedb.operator.Operator;

import java.io.PrintStream;

public class QueryPlan {

    private final Operator rootOperator;

    public QueryPlan(Operator rootOperator) {
        this.rootOperator = rootOperator;
    }

    public Operator getRootOperator() {
        // still see this method
        return rootOperator;
    }

    // change names etc
    public void evaluate(PrintStream printStream) {
        Tuple tuple;
        while ((tuple = rootOperator.getNextTuple()) != null) {
            printStream.println(tuple.toString());
        }
    }
}
