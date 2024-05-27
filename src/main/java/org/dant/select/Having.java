package org.dant.select;

import org.dant.commons.TypeDB;

import java.io.Serializable;
import java.util.List;

public class Having implements Serializable {

    private ColumnSelected aggregate;

    private Condition condition;

    public Having() {
    }

    public Having(ColumnSelected aggregate, Condition condition) {
        this.aggregate = aggregate;
        this.condition = condition;
    }

    public ColumnSelected getAggregate() {
        return aggregate;
    }

    public void setAggregate(ColumnSelected aggregate) {
        this.aggregate = aggregate;
    }

    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    public boolean checkCondition(List<Object> list, int index, String type) {
        switch (aggregate.getTypeAggregat()) {
            case "COUNT" :
                return condition.checkCondition(list, index, TypeDB.INT);
            case "AVG" :
                return condition.checkCondition(list, index, TypeDB.DOUBLE);
            default:
                return condition.checkCondition(list, index, type);
        }
    }
}
