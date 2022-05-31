package org.veupathdb.service.eda.ss.model.filter;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.LongitudeVariable;

import java.util.function.Predicate;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.eda.ss.model.db.DB.Tables.AttributeValue.Columns.NUMBER_VALUE_COL_NAME;

public class LongitudeRangeFilter<T extends Number & Comparable> extends SingleValueFilter<LongitudeVariable, T> {

    private Number _left;
    private Number _right;

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
        if (Math.abs(left - right) < .00000001)
            return " AND 1 = 1" + NL;

        // if  passing thru intl date line, then use OR, else use AND
        //  -50  ================== 0 ==================  50
        //   50  ============== 180/-180 =============== -50
        String op = left < right ? " AND " : " OR ";
        return "  AND (" + NUMBER_VALUE_COL_NAME + " >= " + left + op + NUMBER_VALUE_COL_NAME + " <= " + right + ")" + NL;
    }

    @Override
    public Predicate<T> getPredicate() {
        return n -> n.compareTo(_left) >= 0 && n.compareTo(_right) <= 0;
    }
}
