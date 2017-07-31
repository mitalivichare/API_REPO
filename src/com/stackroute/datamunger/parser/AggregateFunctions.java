package com.stackroute.datamunger.parser;

public class AggregateFunctions 
{
	private String functionName;
	private String columnName;
	private String result;
	
	public AggregateFunctions()
	{}

	public AggregateFunctions(String functionName, String columnName, String result) {
		super();
		this.functionName = functionName;
		this.columnName = columnName;
		this.result = result;
	}

	public String getFunctionName() {
		return functionName;
	}

	public void setFunctionName(String functionName) {
		this.functionName = functionName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	@Override
	public String toString() {
		return "AggregateFunctions [functionName=" + functionName + ", columnName=" + columnName + ", result=" + result
				+ "]";
	}
	
	

}
