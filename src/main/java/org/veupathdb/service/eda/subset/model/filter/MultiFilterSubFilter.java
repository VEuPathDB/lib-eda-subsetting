package org.veupathdb.service.eda.subset.model.filter;

import java.util.List;

import org.veupathdb.service.eda.subset.model.variable.StringVariable;

public class MultiFilterSubFilter {
	private final StringVariable variable;
	private final List<String> stringSet;
	public MultiFilterSubFilter(StringVariable variable, List<String> stringSet) {
		super();
		this.variable = variable;
		this.stringSet = stringSet;
	}
	public StringVariable getVariable() {
		return variable;
	}
	public List<String> getStringSet() {
		return stringSet;
	}
}
