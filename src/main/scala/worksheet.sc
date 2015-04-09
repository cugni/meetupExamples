import com.datastax.spark.connector._
import org.apache.spark.SparkContext._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark._
import sparkbarcelona.{Particle, QuadTree}

//The connector dependencies


// Change here to fit your environment
val cassandraEntryNode="localhost"
val sparkMaster="local[*]"

/** we have to create the spark context with
  * settings for connecting to Cassandra
  * */
val conf=new SparkConf(true)
conf.set("spark.cassandra.connection.host", cassandraEntryNode)

implicit val sc=new SparkContext(sparkMaster,"meetupExamples",conf)
/**
 * If you run this code on a spark cluster, you must add to the
 * SparkContext the spark-connector assembly
 */
if(!sc.isLocal){
  println("It you run this code in a cluster of spark, you must add these jars")
  //Be sure to had run "sbt package" on the project for creating the jar
  sc.addJar("target/scala-2.10/meetupexamples_2.10-1.0.jar")
  //be sure the have built the spark-connector assembly. Refer to the README for this
  sc.addJar("../spark-cassandra-connector/spark-cassandra-connector/target/scala-2.10/spark-cassandra-connector-assembly-1.1.1.jar")
}



println("Better to create the schemas")
//We need to create the keyspace and the tables
CassandraConnector(conf).withSessionDo { session =>
  session.execute("CREATE KEYSPACE IF NOT EXISTS testcase WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
  session.execute("CREATE TABLE IF NOT EXISTS testcase.particles (id int PRIMARY KEY, x float,y float,z float)")
  session.execute("CREATE TABLE IF NOT EXISTS testcase.quad_index (cube text,id int , x float,y float,z float, PRIMARY KEY(cube,id))")
}






println("Generating a random data set")
val sourceSize=100000
val randomOrigin=QuadTree.generateData(sourceSize)

println("Let's give a look to the data.")
randomOrigin.take(5)

println("Note that every time  you call it, the results are different.")
randomOrigin.take(5)
//this method creates automatically the table if doesn't exists
println("Saving the dataset into cassandra")
randomOrigin.saveToCassandra("testcase","particles",SomeColumns("id", "x","y","z"))
val source=sc.cassandraTable[Particle]("testcase","particles") .select("id","x","y","z")
source.count()
println("Converts to Particle automatically")
source.take(5)

println("Let's index: First try")

val s1=System.currentTimeMillis()
val index1=QuadTree.topDown1(source,1000)
index1.count()
val t1=System.currentTimeMillis()-s1
println(s"First index created in $t1 ms")

println("Second try: let's set a bound")
// we try to estimate the maximum size imagining data is uniformly distributed plus 3 levels of margin
val estimatedMaxLevel=(Math.log(sourceSize)/Math.log(8)+1).toInt


val s2=System.currentTimeMillis()
val index2=QuadTree.topDown2(source,maxNodeSize = 1000,estimatedMaxLevel) //max level to 16.. very pessimist
index2.count()
val t2=System.currentTimeMillis()-s2
println(s"Second index created in $t2 ms")


val s3=System.currentTimeMillis()
val index3=QuadTree.bottomUp(source,maxNodeSize = 1000,estimatedMaxLevel) //max level to 16.. very pessimist
index3.count()
val t3=System.currentTimeMillis()-s3
println(s"Last index created in $t3 ms")


println("last but not least, we save the index")

index3.map{
  case (cube,Particle(id,x,y,z))=>(cube,id,x,y,z) //sometime you have to do by hand
}
  .saveToCassandra("testcase","quad_index",SomeColumns("cube","id", "x","y","z"))


println("Some statistics on the indexes")
println("levels in the tree")
index3.map{ case (cube,_)=>cube.length}.stats()
println("elements for cube")
index3.map{case (cube,_)=>(cube,1)}.reduceByKey(_+_).map(_._2).stats()

