# Introduction

This directory contains example app on how to uploads an object using the AWS Java SDK s3 API.

# Configure credentials

In your text editor create a text file  and add the following information 
```
{
[default]
aws_access_key_id = YOUR_AWS_ACCESS_KEY_ID
aws_secret_access_key = YOUR_AWS_SECRET_ACCESS_KEY
}
```
# Creating the project
1. We are going to build this project using maven. This is done by
```
{
    mvn -B archetype:generate \
 -DarchetypeGroupId=org.apache.maven.archetypes \
 -DgroupId=com.example.myapp \
 -DartifactId=myapp
}
```
This creates a directory called myapp and a configuration file pom.xml. Copy and replace the contents of pom.xml with ones in the pom.xml on this github repo to add all the dependencies required.. 

2. Open app.java file and replace the contents with the code on the ap.java file on the git hub repo.

# Building and running the application 

1.To build the project open the project folder and use the following command 
```
{
mvn package
}
```

2. To run the project open the project folder and use the following command 
```
 {
 mvn exec:java -Dexec.mainClass="com.example.myapp.App"
 }
 
```
