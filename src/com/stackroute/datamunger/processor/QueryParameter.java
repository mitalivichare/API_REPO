package com.stackroute.datamunger.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.stackroute.datamunger.data.AggregateFunctions;
import com.stackroute.datamunger.data.Criteria;

public class QueryParameter 
{
	private ArrayList<String> requiredColumnList=null;
	private ArrayList<Criteria> criteriaList=null;
	private ArrayList<AggregateFunctions> aggregateFunctionsList=new ArrayList<>();
	private ArrayList<String> logicalOperatorList=null;
	private String groupByColumn=null;
	private String orderByColumn= null;
	private String fileName;
	private String[] baseQuery;
	private Pattern pattern;
	private Matcher matcher;
	private String countColumn=null;
	private String sumColumn=null;
	private String queryType="SIMPLE_QUERY";
	private int index=0;
	//Criteria criteria=new Criteria();
	//private String baseQuery=null;
	
	
		
	//Method to fill AggregateFunctions Object
			private void fillAggregateFunctions(String columnName,String functionName)
			{
				queryType="AGGREGATE_QUERY";
				AggregateFunctions aggregateFuntion=new AggregateFunctions();
				pattern=Pattern.compile("(\\(([\\w\\*]+)\\))");
				matcher=pattern.matcher(columnName);
				if(matcher.find())
				{
					aggregateFuntion.setColumnName(matcher.group(2));
					aggregateFuntion.setFunctionName(functionName);
				}
				aggregateFunctionsList.add(aggregateFuntion);
			}
			
			
	//Method to extract parameters from the query		
	public QueryParameter extractParameters(String query)
	{
		String[] orderBySplitedList;
		String[] groupBySplitedList;
		String[] whereSplitedList;
		String[] fromSplitedList;
		
			orderBySplitedList=query.split("order by");
			if(orderBySplitedList.length > 1)
			{
				orderByColumn=orderBySplitedList[1].trim();
				queryType="ORDER_BY_QUERY";
			}
			
				groupBySplitedList=orderBySplitedList[0].split("group by");
				if(groupBySplitedList.length > 1)
				{
					groupByColumn=groupBySplitedList[1].trim();
					queryType="GROUP_BY_QUERY";
				}
				
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
					fileName=fromSplitedList[1].trim();
					baseQuery=fromSplitedList[0].split("(\\s|,)+");
					requiredColumnList=new ArrayList<>();
					for(int i=1;i<baseQuery.length;i++)
					{
						if(baseQuery[i].trim().contains("count"))
						{
							fillAggregateFunctions(baseQuery[i],"count");
							/*countArray=baseQuery[i].split("\\(|\\)");
							countColumn=countArray[1].trim();*/
						}
						if(baseQuery[i].trim().contains("sum"))
						{
							fillAggregateFunctions(baseQuery[i],"sum");
							/*countArray=baseQuery[i].split("\\(|\\)");
							sumColumn=countArray[1].trim();*/
						}
						if(baseQuery[i].trim().contains("avg"))
						{
							fillAggregateFunctions(baseQuery[i],"avg");
						}
						if(baseQuery[i].trim().contains("min"))
						{
							fillAggregateFunctions(baseQuery[i],"min");
						}
						if(baseQuery[i].trim().contains("max"))
						{
							fillAggregateFunctions(baseQuery[i],"max");
						}
					}
					for(int i=1;i<baseQuery.length;i++)
					{
						requiredColumnList.add(baseQuery[i].trim());
					}
		
		return this;
	}
	
	//Method to fill Criteria Object
	private void fillCriteria(String criteriaObject)
	{
		queryType="WHERE_QUERY";
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
	
	public int getGroupByColumnIndex()
	{
		int length=requiredColumnList.size();
		for(int i=0; i <length; i++)
		{
			if(requiredColumnList.get(i).equalsIgnoreCase(groupByColumn))
			{
				index=i;
			}
		}
		return index;
	}
	
	public int getOrderByColumnIndex()
	{
		int length=requiredColumnList.size();
		for(int i=0; i < length; i++)
		{
			if(requiredColumnList.get(i).equalsIgnoreCase(orderByColumn));
			{
				index=i;
			}
		}
		return index;
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

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getCountColumn() {
		return countColumn;
	}

	public void setCountColumn(String countColumn) {
		this.countColumn = countColumn;
	}

	public String getSumColumn() {
		return sumColumn;
	}

	public void setSumColumn(String sumColumn) {
		this.sumColumn = sumColumn;
	}
	
	
	public String getQueryType() {
		return queryType;
	}

	public void setQueryType(String queryType) {
		this.queryType = queryType;
	}
	
	
	public ArrayList<AggregateFunctions> getAggregateFunctionsList() {
		return aggregateFunctionsList;
	}
	public void setAggregateFunctionsList(ArrayList<AggregateFunctions> aggregateFunctionsList) {
		this.aggregateFunctionsList = aggregateFunctionsList;
	}
	
	@Override
	public String toString() {
		return "QueryParser [requiredColumnList=" + requiredColumnList + ", criteriaList=" + criteriaList
				+ ", aggregateFunctionsList=" + aggregateFunctionsList + ", logicalOperatorList=" + logicalOperatorList
				+ ", groupByColumn=" + groupByColumn + ", orderByColumn=" + orderByColumn + ", tableName=" + fileName
				+ ", baseQuery=" + Arrays.toString(baseQuery) + ", queryType=" + queryType + "]";
	}

	

	
	
	

}
