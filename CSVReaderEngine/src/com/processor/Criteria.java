package com.processor;

public class Criteria 
{
	private String columnName;
	private String operator;
	private String value;
	
	public Criteria()
	{}
	public Criteria(String columnName, String operator, String value) {
		super();
		this.columnName = columnName;
		this.operator = operator;
		this.value = value;
	}
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getOperator() {
		return operator;
	}
	public void setOperator(String operator) {
		this.operator = operator;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	@Override
	public String toString() {
		return "Criteria [columnName=" + columnName + ", operator=" + operator + ", value=" + value + "]";
	}
	
	

}
