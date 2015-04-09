# Indexing 3-dimensional trajectories: Apache Spark and Cassandra integration
This repository contains examples shown  the 8th of April 2015 at the Barcelona Spark Meetup. 

Happy hacking!

##Build for running on a Spark cluster
1. Build the spark-cassandra-connector assembly. If we run the code on a Spark cluster, we will need it. 

   ```
   git clone git@github.com:datastax/spark-cassandra-connector.git
   cd spark-cassandra-connector
   git fetch --tags  //downloads all the tags from the Github repository
   git checkout v1.1.0  //Select the version we are using
   sbt assembly   
   // generates the assembly of the driver in
   //
   ```
2. Package our code. We will have to load it too
   ```
   cd ../meetupExamples/
   sbt package
   ```
   
##Run the code
We created a worksheet with some example. You can decide to run it from a REPL console: 

   ```
   sbt console
   scala> :load src/main/scala/worksheet.sc  //so you can load the script and enjoy the view
   ``` 
   or in the spark-shell
   ```
   bin/spark-shell --jars ../connector.jar,../exampleCode.jar

   ```
##Play with the code!
   Open *worksheet.sc* and *QuadTree.scala*, look to the code, play with the parameters. 
   You can change the logging levels in *src/main/resources/log4j.properties* to better understanding what is going on. (Set it to INFO at the beginning.)
   
##Improve this demo
when you find an error, *clone* the repo, *fix* it and *push*!
  

