package com.util.loader;

//Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.


import java.io.File;
import java.util.Iterator;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.util.constants.Constants;

public class MoviesLoadData {

	// run with one argument as region
 public static void main(String[] args) throws Exception {

	 String region = args[0];
     AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
         .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(Constants.LOCAL_HOST, region))
         .build();

     DynamoDB dynamoDB = new DynamoDB(client);

     Table table = dynamoDB.getTable(Constants.TABLE_NAME);
   
     JsonParser parser = new JsonFactory().createParser(new File(MoviesLoadData.class.getResource("moviesdata.json").toString()));

     JsonNode rootNode = new ObjectMapper().readTree(parser);
     Iterator<JsonNode> iter = rootNode.iterator();

     ObjectNode currentNode;

     while (iter.hasNext()) {
         currentNode = (ObjectNode) iter.next();

         int year = currentNode.path("year").asInt();
         String title = currentNode.path("title").asText();

         try {
             table.putItem(new Item().withPrimaryKey("year", year, "title", title).withJSON("info",
                 currentNode.path("info").toString()));
             System.out.println("PutItem succeeded: " + year + " " + title);

         }
         catch (Exception e) {
             System.err.println("Unable to add movie: " + year + " " + title);
             System.err.println(e.getMessage());
             break;
         }
     }
     parser.close();
 }
}