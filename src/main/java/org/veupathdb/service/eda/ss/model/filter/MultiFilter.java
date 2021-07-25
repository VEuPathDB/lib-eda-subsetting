package org.veupathdb.service.eda.ss.model.filter;

import java.util.ArrayList;
import java.util.List;

import static org.gusdb.fgputil.FormatUtil.NL;
import org.veupathdb.service.eda.ss.model.Entity;

public class MultiFilter extends Filter {
	
	private final List<MultiFilterSubFilter> subFilters;
	private final MultiFilterOperation operation;
	
	public enum MultiFilterOperation {
		UNION("UNION"), INTERSECT("INTERSECT");

		private final String name;

		MultiFilterOperation(String name) {
			this.name = name;
		}
		
		String getName() { return name; }
		
	    public static MultiFilterOperation fromString(String operation) {

	    	MultiFilterOperation o;

	        switch (operation) {
	          case "intersect" -> o = INTERSECT;
	          case "union" -> o = UNION;
	          default -> throw new RuntimeException("Unrecognized multi-filter operation: " + operation);
	        }
	        return o;
	      }

	}
	
	@Override
	public String getSql() {
		List<String> subFiltersSqlList = new ArrayList<String>();
		for (MultiFilterSubFilter subFilter : subFilters) subFiltersSqlList.add(getSingleFilterSql(subFilter));
		String subFiltersSql = String.join("  " + operation.getName() + NL, subFiltersSqlList);
		return "  select * from ( -- START OF MULTIFILTER" + NL
				+ subFiltersSql + NL
				+ "  ) -- END OF MULTIFILTER";
	}

	public MultiFilter(Entity entity, List<MultiFilterSubFilter> subFilters, MultiFilterOperation operation) {
		super(entity);
		this.subFilters = subFilters;
		this.operation = operation;
	}
	
	private String getSingleFilterSql(MultiFilterSubFilter subFilter) {
		StringSetFilter ssf = new StringSetFilter(entity, subFilter.getVariable().getId(), subFilter.getStringSet());
		return ssf.getSql();
	}

}
