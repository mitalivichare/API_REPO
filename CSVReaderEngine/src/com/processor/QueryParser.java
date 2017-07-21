package com.processor;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryParser 
{
	private ArrayList<String> requiredColumnList=null;
	private ArrayList<Criteria> criteriaList=null;
	private ArrayList<String> logicalOperatorList=null;
	private String groupByColumn=null;
	private String orderByColumn= null;
	private String sumFunction=null;
	private String countFunction=null;
	private String tableName;
	private String[] baseQuery;
	private Pattern pattern;
	private Matcher matcher;
	//Criteria criteria=new Criteria();
	//private String baseQuery=null;

	public QueryParser extractParameters(String query)
	{
		String[] orderBySplitedList;
		String[] groupBySplitedList;
		String[] whereSplitedList;
		String[] fromSplitedList;
		if(query.contains("select") && query.contains("from"))
		{
			orderBySplitedList=query.split("order by");
			if(orderBySplitedList.length > 1)
			{
				orderByColumn=orderBySplitedList[1].trim();
			}
			else
			{
				groupBySplitedList=orderBySplitedList[0].split("group by");
				if(groupBySplitedList.length > 1)
				{
					groupByColumn=groupBySplitedList[1].trim();
				}
				else
				{
					whereSplitedList=groupBySplitedList[0].split("where");
					if(whereSplitedList.length > 1)
					{
						criteriaList=new ArrayList<>();
						logicalOperatorList=new ArrayList<>();
						String criteriaString=whereSplitedList[1].trim();//store the criteria in a string object
						String[] criteriaArray=criteriaString.split("\\s+"); //seperate all components and store it in an array for checking presence of and/or
						for(String string : criteriaArray)
						{
							if(string.equals("and"))
								logicalOperatorList.add("and");
							else if(string.equals("or"))
								logicalOperatorList.add("or");
						}
						String [] whereComponents=criteriaString.split(" and | or ",2);
						while(whereComponents.length != 1)
						{
							fillCriteria(whereComponents[0].trim());
							whereComponents=whereComponents[1].trim().split(" and | or ", 2);
						}
						fillCriteria(whereComponents[0].trim());
					}
					fromSplitedList=whereSplitedList[0].split("from");
					tableName=fromSplitedList[1].trim();
					baseQuery=fromSplitedList[0].split("(\\s|,)+");
					requiredColumnList=new ArrayList<>();
					for(int i=1;i<baseQuery.length;i++)
					{
						requiredColumnList.add(baseQuery[i].trim());
					}
				}
				
			}
			
			
		}
		
		return this;
	}
	
	private void fillCriteria(String criteriaObject)
	{
		Criteria criteria=new Criteria();
		pattern=Pattern.compile("(.*) ([!=|>=|<=|>|<|=]+) (.*)");
		matcher=pattern.matcher(criteriaObject);
		if(matcher.find())
		{
			criteria.setColumnName(matcher.group(1).trim());
			criteria.setOperator(matcher.group(2).trim());
			criteria.setValue(matcher.group(3).trim());
		}
		criteriaList.add(criteria);
	}

	public ArrayList<String> getRequiredColumnList() {
		return requiredColumnList;
	}

	public void setRequiredColumnList(ArrayList<String> requiredColumnList) {
		this.requiredColumnList = requiredColumnList;
	}

	public ArrayList<Criteria> getCriteriaList() {
		return criteriaList;
	}

	public void setCriteriaList(ArrayList<Criteria> criteriaList) {
		this.criteriaList = criteriaList;
	}

	public ArrayList<String> getLogicalOperatorList() {
		return logicalOperatorList;
	}

	public void setLogicalOperatorList(ArrayList<String> operatorList) {
		this.logicalOperatorList = operatorList;
	}

	public String getGroupByColumn() {
		return groupByColumn;
	}

	public void setGroupByColumn(String groupByColumn) {
		this.groupByColumn = groupByColumn;
	}

	public String getOrderByColumn() {
		return orderByColumn;
	}

	public void setOrderByColumn(String orderByColumn) {
		this.orderByColumn = orderByColumn;
	}

	public String getSumFunction() {
		return sumFunction;
	}

	public void setSumFunction(String sumFunction) {
		this.sumFunction = sumFunction;
	}

	public String getCountFunction() {
		return countFunction;
	}

	public void setCountFunction(String countFunction) {
		this.countFunction = countFunction;
	}

	public String geTableName() {
		return tableName;
	}

	public void setTableName(String fileName) {
		this.tableName = fileName;
	}

	@Override
	public String toString() {
		return "QueryParser [requiredColumnList=" + requiredColumnList + ", criteriaList=" + criteriaList
				+ ", logicalOperatorList=" + logicalOperatorList + ", groupByColumn=" + groupByColumn
				+ ", orderByColumn=" + orderByColumn + ", sumFunction=" + sumFunction + ", countFunction="
				+ countFunction + ", tableName=" + tableName + "]";
	}
	
	

}
