package GetStarted;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import org.apache.commons.io.IOUtils;
import java.io.*;
import java.io.File;
import java.sql.Timestamp;

/**
 * Simple application that shows how to use Azure Cosmos DB for MongoDB API in a Java application.
 *
 */
public class Program {

    public static void main(String[] args)
    {
				/*
				* Replace connection string from the Azure Cosmos DB Portal
        */

        MongoClientURI uri = new MongoClientURI("mongodb://");

        MongoClient mongoClient = null;
        try {
            mongoClient = new MongoClient(uri);

            // Get database
            MongoDatabase database = mongoClient.getDatabase("auditNonPrd");
						//MongoDatabase database = mongoClient.getDatabase("testDb");

            // Get collection
            MongoCollection<Document> collection = database.getCollection("oasisAuditDev");


						if (args.length > 0) {
							System.out.println("Doing " + args[0]);

							long startTime;
							long endTime;

							if (args[0].equals("delete")) {
								// removing all audit records
								// query for all userId's

								for (Document cur : collection.find().projection(new Document("userId", 1))) {
									System.out.println(cur.toJson());
									collection.deleteMany(Filters.eq("userId", cur.get("userId")));
								}
							}

							if (args[0].equals("queryUser")) {
								// Query for all records of userId
								String userId = args[1];

								// query 10 times
								for (int i=0; i<10; i++) {
									startTime = System.currentTimeMillis();
									for (Document cur : collection.find(Filters.eq("userId", userId)).projection(new Document("userId", 1).append("estateId", 1))) {
										System.out.println(cur.toJson());
									}
									endTime = System.currentTimeMillis();
									System.out.println("Find Time: " + (endTime - startTime) + " milliseconds");
								}
							}

							if (args[0].equals("queryEstate")) {
								// Query for all records of userId
								String estateId = args[1];

								// query 10 times
								for (int i=0; i<10; i++) {
									startTime = System.currentTimeMillis();
									for (Document cur : collection.find(Filters.eq("estateId", estateId)).projection(new Document("userId", 1).append("estateId", 1))) {
										System.out.println(cur.toJson());
									}
									endTime = System.currentTimeMillis();
									System.out.println("Find Time: " + (endTime - startTime) + " milliseconds");
								}
							}

						} else {

							// Set up dummy audit record
							File f = new File("/Users/sampipe/Documents/Work/Foster_Moore/git/azure-cosmos-db-mongodb-java-getting-started/src/GetStarted/auditRecord.txt");
							if (f.exists()){
								String jsonTxt = new String();
								try{
									 	InputStream is = new FileInputStream( "/Users/sampipe/Documents/Work/Foster_Moore/git/azure-cosmos-db-mongodb-java-getting-started/src/GetStarted/auditRecord.txt");
										jsonTxt = IOUtils.toString( is, "UTF-8");
										System.out.println("AuditRecordString\n" + jsonTxt);
								} catch (Exception e) {
										System.out.println("exception got");
								}
	            	JSONObject auditRecordJson = (JSONObject) JSONSerializer.toJSON( jsonTxt );

								long startTime;
								long endTime;
								long totalTime = 0;
								long avgTime;


								// Now insert a number of audit records and take timings.
								Document auditRecord1 = new Document();
								for (int i=0; i<400; i++) {

									// generate random number so we get random userId added.
									int random40 = (int)(Math.random()*40);
									int random100 = (int)(Math.random()*100);
									// update into auditRecord1
									auditRecordJson.put("userId", "userId" + random40);
									auditRecordJson.put("estateId", "5000" + random100);
									// update TIMESTAMP
									Timestamp timestamp = new Timestamp(System.currentTimeMillis());
									auditRecordJson.put("timestamp", timestamp.toString());

									// create mongoDB document
									auditRecord1 = Document.parse(auditRecordJson.toString());

									startTime = System.currentTimeMillis();
									//System.out.println(startTime);
									collection.insertOne(auditRecord1);
									endTime = System.currentTimeMillis();
									//System.out.println(endTime);

									System.out.println(i + " UserId: userId" + random40 + " , EstateId: 5000" + random100 + " , Insert Time: " + (endTime - startTime) + " milliseconds");
									if (i!=0) {
										totalTime = totalTime + (endTime - startTime);
									}
								}

								System.out.println("Average Time: " + (totalTime/399) + " milliseconds");
								System.out.println("Total Time: " + totalTime + " milliseconds");

							} else {
								System.out.println("File doesn't exist.");
							}
						}

            // Insert documents
            //Document document1 = new Document("fruit", "apple");
            //collection.insertOne(document1);

            //Document document2 = new Document("fruit", "mango");
            //collection.insertOne(document2);

            // Find fruits by name
            //Document queryResult = collection.find(Filters.eq("fruit", "apple")).first();
            //System.out.println(queryResult.toJson());

						// Find all fruit
						//for (Document cur : collection.find()) {
						//	System.out.println(cur.toJson());
						//}

						// Supported commands = https://docs.microsoft.com/en-us/azure/cosmos-db/mongodb-feature-support
						//Document buildInfoResults = database.runCommand(new Document("buildInfo", 1));
						//System.out.println(buildInfoResults.toJson());

						//Document collStatsResults = database.runCommand(new Document("collStats", "coll"));
						//System.out.println(collStatsResults.toJson());



            System.out.println( "Completed successfully" );

        } finally {
        	if (mongoClient != null) {
        		mongoClient.close();
        	}
        }
    }
}
