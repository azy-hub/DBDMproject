package org.dant.select;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.dant.commons.TypeDB;

import java.math.BigDecimal;
import java.util.List;

public class Having {

    @JsonProperty("aggregate")
    private ColumnSelected aggregate;
    @JsonProperty("condition")
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

    public boolean checkHaving(List<Object> list, int index, String type) {
        return switch (aggregate.getTypeAggregat()) {
            case "COUNT" -> condition.checkCondition(list, index, TypeDB.INT);
            case "AVG" -> condition.checkCondition(list, index, TypeDB.DOUBLE);
            default -> condition.checkCondition(list, index, type);
        };
    }

    public boolean checkCondition() {
        return switch (aggregate.getTypeAggregat()) {
            case "COUNT" ->
                    ((condition.getValue() instanceof BigDecimal) && ((BigDecimal) condition.getValue()).stripTrailingZeros().scale() <= 0);
            case "AVG", "SUM" -> condition.getValue() instanceof BigDecimal;
            default -> true;
        };
    }
}
