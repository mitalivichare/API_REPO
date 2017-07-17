package processor;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryParser 
{
	private String requiredColumnList[]=null;
	private ArrayList<Criteria> criteriaList=null;
	private ArrayList<String> operatorList;
	private String groupByColumn=null;
	private String orderByColumn= null;
	private String sumFunction=null;
	private String countFunction=null;
	private String fileName=null;
	private Pattern pattern;
	private Matcher matcher;
	
	public QueryParser parameterProcessing(String query)
	{
		return null;
	}

	public String[] getRequiredColumnList() {
		return requiredColumnList;
	}

	public void setRequiredColumnList(String[] requiredColumnList) {
		this.requiredColumnList = requiredColumnList;
	}

	public ArrayList<Criteria> getCriteriaList() {
		return criteriaList;
	}

	public void setCriteriaList(ArrayList<Criteria> criteriaList) {
		this.criteriaList = criteriaList;
	}

	public ArrayList<String> getOperatorList() {
		return operatorList;
	}

	public void setOperatorList(ArrayList<String> operatorList) {
		this.operatorList = operatorList;
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

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public Pattern getPattern() {
		return pattern;
	}

	public void setPattern(Pattern pattern) {
		this.pattern = pattern;
	}

	public Matcher getMatcher() {
		return matcher;
	}

	public void setMatcher(Matcher matcher) {
		this.matcher = matcher;
	}
	
	

}
