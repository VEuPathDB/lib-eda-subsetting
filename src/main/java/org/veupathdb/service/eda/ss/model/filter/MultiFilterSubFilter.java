package org.veupathdb.service.eda.ss.model.filter;

import java.util.List;

import org.veupathdb.service.eda.ss.model.variable.StringVariable;
import org.veupathdb.service.eda.ss.model.variable.Variable;

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
