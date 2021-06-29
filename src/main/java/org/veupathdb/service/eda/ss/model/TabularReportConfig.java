package org.veupathdb.service.eda.ss.model;

import java.util.List;

public class TabularReportConfig {
	  private List<String> sortingVariableIds;
	  private Integer numRows;
	  private Integer offset;
	  
	public TabularReportConfig(List<String> sortingVariableIds, Integer numRows, Integer offset) {
		this.sortingVariableIds = sortingVariableIds;
		this.numRows = numRows;
		this.offset = offset;
	}

	public List<String> getSortingVariableIds() {
		return sortingVariableIds;
	}

	public Integer getNumRows() {
		return numRows;
	}

	public Integer getOffset() {
		return offset;
	}
	  
}
