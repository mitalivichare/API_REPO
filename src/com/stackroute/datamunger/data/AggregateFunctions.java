package com.stackroute.datamunger.data;

public class AggregateFunctions 
{
	private String functionName;
	private String columnName;
	private double result;
	
	public AggregateFunctions()
	{}

	public AggregateFunctions(String functionName, String columnName, Double result) {
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

	public double getResult() {
		return result;
	}

	public void setResult(double result) {
		this.result = result;
	}

	@Override
	public String toString() {
		return "AggregateFunctions [functionName=" + functionName + ", columnName=" + columnName + ", result=" + result
				+ ", getResult()=" + getResult() + "]";
	}

	
	
	

}
