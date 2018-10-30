package com.util.dynamodb;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.sns.AmazonSNSClient;
public class CRUDOperations {

	
	 public static void main(String[] args) throws Exception {

	        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
	            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
	            .build();

	        DynamoDB dynamoDB = new DynamoDB(client);

	        Table table = dynamoDB.getTable("Movies");

	        
	      
	        int year = 2015;
	        String title = "The Big New Movie";
	        insertItem(table,year,title);
	        
	        updateItem(table,year,title);
	       
	        updateConditional(table,year,title);
	        
	        deleteItem(table, year, title);
	    }

	private static void updateConditional(Table table, int year, String title) {
		
		UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("year",year,"title",title).withUpdateExpression("remove info.actors[0]").
				withConditionExpression("size(info.actors) >= :num").withValueMap(new ValueMap().withNumber(":num", 3)).withReturnValues(ReturnValue.UPDATED_NEW);
		
		try
		{
			
			System.out.println("Updating Items ");
			UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
			System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
			
		}
		catch(Exception e){
			  System.err.println("Unable to update item: " + year + " " + title);
	          System.err.println(e.getMessage());
		
		}
		

		 updateItemSpec = new UpdateItemSpec().withPrimaryKey("year",year,"title",title).withUpdateExpression("set info.actors[0] = :newac").
				withConditionExpression("size(info.actors) >= :num").withValueMap(new ValueMap().withNumber(":num", 2).withString(":newac", "Tom")).withReturnValues(ReturnValue.UPDATED_NEW);
		
		
		 try
			{
				
				System.out.println("Updating Items ");
				UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
				System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
				
			}
			catch(Exception e){
				  System.err.println("Unable to update item: " + year + " " + title);
		          System.err.println(e.getMessage());
			
			}
	}

	private static void updateItem(Table table, int year, String title) {
		
		
		UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("year", year, "title", title)
				
				// set,add,delete and set if exists
	            .withUpdateExpression("set info.rating = :r, info.plot=:p, info.actors=:a")
	            .withValueMap(new ValueMap().withNumber(":r", 5.5).withString(":p", "Everything happens all at once.")
	                .withList(":a", Arrays.asList("Larry", "Moe", "Curly")))
	            .withReturnValues(ReturnValue.UPDATED_OLD);

	        try {
	            System.out.println("Updating the item...");
	            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
	            System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

	        }
	        catch (Exception e) {
	            System.err.println("Unable to update item: " + year + " " + title);
	            System.err.println(e.getMessage());
	        }
	}

	private static void insertItem(Table table, int year, String title) {
		

		 final Map<String, Object> infoMap = new HashMap<String, Object>();
	        infoMap.put("plot", "Nothing happens at all.");
	        infoMap.put("rating", 0);

	        try {
	        	
	        	//check if exists
	        	
	        	boolean isExist = table.getItem(new PrimaryKey("year",year,"title",title))==null?false:true;
	        	if(isExist)
	        	{
	        		System.err.println("Item exists already");
	        		return;
	        	}
	            System.out.println("Adding a new item...");
	            PutItemOutcome outcome = table
	                .putItem(new Item().withPrimaryKey("year", year, "title", title).withMap("info", infoMap));

	            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());

	        }
	        catch (Exception e) {
	            System.err.println("Unable to add item: " + year + " " + title);
	            System.err.println(e.getMessage());
	        }
		
	}
	
	private static void deleteItem(Table table,int year,String title)
	{
		DeleteItemSpec delSpec = new DeleteItemSpec().withPrimaryKey("year", year, "title", title);		
		 try {
			 	System.out.println(table.getItem(new PrimaryKey("year",year,"title",title))==null);
	            System.out.println("Attempting a conditional delete...");
	            table.deleteItem(delSpec);
	            System.out.println("DeleteItem succeeded");
	            System.out.println(table.getItem(new PrimaryKey("year",year,"title",title))==null);
	        }
	        catch (Exception e) {
	            System.err.println("Unable to delete item: " + year + " " + title);
	            System.err.println(e.getMessage());
	        }
	}
}
