CLONE OP REPO https://github.com/Azure-Samples/azure-cosmos-db-mongodb-java-getting-started

Modified for My testing.

Update the mongodb connection string in the Programs.java file.

build with:
* mvn package

run with:
* mvn exec:java -D exec.mainClass=GetStarted.Program (to insert 400 records and take timing)
* mvn exec:java -D exec.mainClass=GetStarted.Program -Dexec.args="queryEstate 500090" (to run some queries for this estate number)
* mvn exec:java -D exec.mainClass=GetStarted.Program -Dexec.args="queryUser userId20" (to run some queries for this userId)
* mvn exec:java -D exec.mainClass=GetStarted.Program -Dexec.args="delete" (to delete all records from the DB)







---
services: cosmos-db
platforms: java
author: arramac
---

# Developing a Java app using Azure Cosmos DB for MongoDB API
Azure Cosmos DB is a fully managed globally distributed, multi-model database service, transparently replicating your data across any number of Azure regions. You can elastically scale throughput and storage, and take advantage of fast, single-digit-millisecond data access using the API of your choice backed by 99.999 SLA. This sample shows you how to use the Azure Cosmos DB for MongoDB API to store and access data from a Java application.

## Running this sample

* Before you can run this sample, you must have the following prerequisites:

   * An active Azure account. If you don't have one, you can sign up for a [free account](https://azure.microsoft.com/free/). Alternatively, you can use the [Azure Cosmos DB Emulator](https://docs.microsoft.com/azure/cosmos-db/local-emulator) for this tutorial.
   * JDK 1.7+ (Run `apt-get install default-jdk` if you don't have JDK)
   * Maven (Run `apt-get install maven` if you don't have Maven)

* Then, clone this repository using `git clone git@github.com:azure-samples/azure-cosmosdb-for-mongodb-api-java-getting-started.git`

* Next, substitute the endpoint and authorization key in Program.java with your Cosmos DB account's values.

* From a command prompt or shell, run `mvn package` to compile and resolve dependencies.

* From a command prompt or shell, run `mvn exec:java -D exec.mainClass=GetStarted.Program` to run the application.

## About the code
The code included in this sample is intended to get you quickly started with a Java application that connects to Azure Cosmos DB for MongoDB API.

## More information

- [Azure Cosmos DB](https://docs.microsoft.com/azure/cosmos-db/introduction)
- [Azure Cosmos DB for MongoDB API](https://docs.microsoft.com/en-us/azure/cosmos-db/mongodb-introduction)
- [MongoDB Java driver](https://docs.mongodb.com/ecosystem/drivers/java/)
