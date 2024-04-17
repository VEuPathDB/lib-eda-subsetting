package org.veupathdb.service.eda.subset.model.filter;

import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.variable.LongitudeVariable;
import org.veupathdb.service.eda.subset.model.db.DB;

import java.util.function.Predicate;

import static org.gusdb.fgputil.FormatUtil.NL;

public class LongitudeRangeFilter extends SingleValueFilter<Double, LongitudeVariable> {
    private static final double EPSILON = .00000001;

    private final Number _left;
    private final Number _right;

    public LongitudeRangeFilter(String appDbSchema, Entity entity, LongitudeVariable variable, Number left, Number right) {
        super(appDbSchema, entity, variable);
        _left = left;
        _right = right;
    }

    // safe from SQL injection since input classes are Number
    @Override
    public String getFilteringAndClausesSql() {
        double left = _left.doubleValue();
        double right = _right.doubleValue();

        // if left and right are the same, or very close to it, then apply nop filter
        if (Math.abs(left - right) < EPSILON)
            return " AND 1 = 1" + NL;

        // if  passing thru intl date line, then use OR, else use AND
        //  -50  ================== 0 ==================  50
        //   50  ============== 180/-180 =============== -50
        String op = left < right ? " AND " : " OR ";
        return "  AND (" + DB.Tables.AttributeValue.Columns.NUMBER_VALUE_COL_NAME + " >= " + left + op + DB.Tables.AttributeValue.Columns.NUMBER_VALUE_COL_NAME + " <= " + right + ")" + NL;
    }

    @Override
    public Predicate<Double> getPredicate() {
        return num -> {
            double left = _left.doubleValue();
            double right = _right.doubleValue();

            if (Math.abs(left - right) < EPSILON) {
                return true;
            }

            if (left < right) {
                return (num >= left && num <= right);
            } else {
                return (num >= left || num <= right);
            }
        };
    }
}
