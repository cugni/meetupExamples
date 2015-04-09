# Indexing 3-dimensional trajectories: Apache Spark and Cassandra integration
This repository contains examples shown  the 8th of April 2015 at the Barcelona Spark Meetup. 

Happy hacking!

##Build to run on a cluster
1. First, you need to build the spark-cassandra-connector in order to run on a spark cluster

   Make the Assembly of the connector
```
git clone git@github.com:datastax/spark-cassandra-connector.git
cd spark-cassandra-connector
git fetch --tags  //downloads all the tags from the Github repository
git checkout v1.1.1  //Select the version we are using
sbt assembly   
// generates the assembly of the driver in
//
```
2. Package the our code
```
cd ../meetupExamples/
sbt package
```
3. Run the worksheets
   You can decide to run the code from a REPL console
   ```
   sbt console
   scala> :load src/main/scala/worksheet.sc  //so you can load the script and enjoy the view
   ``` 
   or in the spark-shell
   ```
   bin/spark-shell --jars ../connector.jar,../exampleCode.jar

   ```
4. Play with the code!
   Open worksheet.sc and QuadTree.scala, look to the code, play with the parameters. You can change the logging levels
   in    src/main/resources/log4j.properties to better understand what is going on. Set it to INFO at the beginning. 
   
5. Improve this demo: when you find a error, clone, fish and push!
  

