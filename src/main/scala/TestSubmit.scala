import com.datastax.spark.connector._
import org.apache.spark.SparkContext._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark._
import sparkbarcelona.{Particle, QuadTree}

//The connector dependencies
object TestSubmit extends App {

  /** we have to create the spark context with
    * settings for connecting to Cassandra
    * */
  val conf = new SparkConf(true)
  implicit val sc = new SparkContext(conf)

  println("Better to create the schemas")
  //We need to create the keyspace and the tables
  CassandraConnector(conf).withSessionDo { session =>
    session.execute("CREATE KEYSPACE IF NOT EXISTS testcase WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE IF NOT EXISTS testcase.particles (id int PRIMARY KEY, x float,y float,z float)")
    session.execute("CREATE TABLE IF NOT EXISTS testcase.quad_index (cube text,id int , x float,y float,z float, PRIMARY KEY(cube,id))")
    session.execute("TRUNCATE testcase.particles") //let's start with empty tables
    session.execute("TRUNCATE testcase.quad_index")

  }


  val sourceSize = Integer.getInteger(  "sourceSize",100000)
  println(s"Generating a random data set of size $sourceSize")
  val randomOrigin = QuadTree.generateData(sourceSize)

  println("Note that every time  you call it, the results are different.")
  randomOrigin.take(5)
  //this method creates automatically the table if doesn't exists
  println("Saving the dataset into cassandra")
  randomOrigin.saveToCassandra("testcase", "particles", SomeColumns("id", "x", "y", "z"))
  val source = sc.cassandraTable[Particle]("testcase", "particles").select("id", "x", "y", "z")
  source.count()
  val estimatedMaxLevel=(Math.log(sourceSize.toDouble)/Math.log(8)+1).toInt

  val s3 = System.currentTimeMillis()
  val index3 = QuadTree.bottomUp(source, maxNodeSize = 1000, estimatedMaxLevel) //max level to 16.. very pessimist
  index3.count()
  val t3 = System.currentTimeMillis() - s3
  println(s"Last index created in $t3 ms")


  println("last but not least, we save the index")

  index3.map {
    case (cube, Particle(id, x, y, z)) => (cube, id, x, y, z) //sometime you have to do by hand

  }.saveToCassandra("testcase", "quad_index", SomeColumns("cube", "id", "x", "y", "z"))


  println("Some statistics on the indexes")
  println("levels in the tree")
  val cubeLengths = index3.map { case (cube, _) => cube.length }.stats()
  println("elements for cube")
  val cubeSizes = index3.map { case (cube, _) => (cube, 1) }.reduceByKey(_ + _).map(_._2).stats()

}