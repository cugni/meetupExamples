package sparkbarcelona

//adds the spark connector functionality
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

 import scala.util.Random

case class Particle(id:Int,x:Float,y:Float,z:Float)


object QuadTree {





  /**
   * Generates a random data set.  You mai pass the spark context explicitly
   * or implicitly mark it (e.g implicit val isc= sc)
   * @param size number of particles
   * @param sc Spark context
   * @return and RDD of 3d Particles
   */
  def generateData(size:Int=10000)(implicit sc:SparkContext):RDD[Particle]={
    val root=Math.round(Math.sqrt(size)).toInt
    // In this way should scale better generating very huge sources
    sc.parallelize(0 until root)
      .flatMap(id=>
      (0 to root).map(off=>Particle(id*root+off,
        // We use Gaussian distribution because it's easy: actual data is not so nicely distributed
        Random.nextGaussian().toFloat,
        Random.nextGaussian().toFloat,
        Random.nextGaussian().toFloat)
      ))
  }

  /** *
    * The first logical attempt. It starts from the root, it counts the elements
    * for each cube and if there are cubes with more than maxNodeSize elements it
    * continues iterating
    * @param source
    * @param maxNodeSize
    * @return
    */
  def topDown1(source:RDD[Particle],maxNodeSize:Int=1000):RDD[(String,Particle)]={

     val startLevel = source.map(particle => ("", particle))


      def recurQuad(toBeSplit: RDD[(String, Particle)]):RDD[(String,Particle)] = {
          val rec = toBeSplit.map{  case (key,particle) => (key, 1)  }.reduceByKey(_ + _)

          val counted = toBeSplit.join(rec)
          //if the father outreach maxNodeSize, then store the sons
          val emptyCubes = counted.filter{
            case ((cube,(particle,count)) )=>   count <= maxNodeSize
          }.map {
            case ((cube,(particle,count)))=>(cube,particle) //we reset the rdd shape
          }

          val fullCubes = counted.filter{
            case ((cube,(particle,count)))=> count  > maxNodeSize
          }map{
            case (cube, (particle, _)) => (CubeFactory.createSon(cube,particle), particle)
          }


          /** *
          * Here is the bad! We collect to the driver the count and then we decide
            * if to proceed with another iteration=> very slow and few parallelism
          */
          if(fullCubes.count()>0)
            emptyCubes.union(recurQuad(fullCubes))
          else
            emptyCubes

        }
      recurQuad(startLevel)

  }

  /** *
    * A second attempt. We want to avoid to count to the driver, so we set a maximum number of level possible.
    * Note that differently form the fist attempt, this method returns and RDD which is not yet calculated.
    * @param source
    * @param maxNodeSize
    * @return
    */
  def topDown2(source:RDD[Particle],maxNodeSize:Int=1000, maxLevels:Int=10):RDD[(String,Particle)]={

    val startLevel = source.map(particle => ("", particle))
     def recurQuad(toBeSplit: RDD[(String, Particle)], level: Int): RDD[(String, Particle)] = {

      if(level==0){ //We can stop to recur
        //Returns the to input RDD
        toBeSplit
      }else {
        /**
         * Now the problem is here: In the first iterations I have only a few distinct
         * cubes: the most of the data will be shuffle to few Spark worker and it will
         * probably result in out of memory execution.
         */
        val rec = toBeSplit.map { case (cube, particle) => (cube, 1) }.reduceByKey(_ + _)

        val counted = toBeSplit.join(rec)
        //if the father outreach 1024, then store the sons
        val fittingCubes = counted.filter {
          case ((cube, (particle, count))) => count <= maxNodeSize
        }.map {
          case ((cube, (particle, count))) => (cube, particle)
        }
        val fullCubes = counted.filter {
          case ((cube, (particle, count))) => count > maxNodeSize
        } map {
          case (cube, (particle, _)) => (CubeFactory.createSon(cube, particle), particle)
        }

          fittingCubes.union(recurQuad(fullCubes,  level - 1))

      }
    }
    recurQuad(startLevel, maxLevels)

  }



  /** *
    * Third and last attempt. We want to avoid to have hotspot in the data, so we construct the index from the bottom,
    * so that the first iteration, the ones which handle more data, are executed uniformly between the spark workers.
    * @param source
    * @param maxNodeSize
    * @return
    */
  def bottomUp(source:RDD[Particle],maxNodeSize:Int=1000, maxLevels:Int=10):RDD[(String,Particle)]={

    val startLevel = source.map(particle => (CubeFactory.cube(particle,maxLevels), particle))
    //We count how many particles fall in the same cube.
    val startRec=startLevel.map{case (cube,particle)=>(cube,1)}.reduceByKey(_+_)


    def recurQuad(toBeSplit: RDD[(String, Particle)],recurrence:RDD[(String,Int)], level: Int): RDD[(String, Particle)] = {

      if(level==0){
        //Returns the to input RDD
         toBeSplit
      }else {
        val counted = toBeSplit.join(recurrence)

        //If the cube is not full, we try to fill the father
        val toBeReducedAgain = counted.filter {
          case ((cube, (particle, count))) => count <= maxNodeSize
        }.map {
          case ((cube, (particle, count))) => (cube.substring(0,cube.length-1), particle)
        }

        val fullCubes = counted.filter {
          case ((cube, (particle, count))) => count > maxNodeSize
        } map {
          //I've to split the cube again, because it has more elements than the maximum allowed.
          case (cube, (particle, _)) => (CubeFactory.createSon(cube, particle), particle)
        }
         /**
         * Here we can reuse the previously aggregated data for preparing  recurrences of cubes for the next iteration.
         */
        val rec=recurrence.map {
          case (cube, rec) => (cube.substring(0,cube.length-1), rec)
        }.reduceByKey(_ + _)

        //let's  do another iteration
        fullCubes.union(recurQuad(toBeReducedAgain,rec, level - 1))

      }
    }
    recurQuad(startLevel,startRec, maxLevels)

  }





}



/**
 * The *magic* bit logic
 */
object CubeFactory extends Serializable {


  def cube(point: Particle, times: Int): String =
    cube(point.x, point.y, point.z, times)

  def cube(x: Float, y: Float, z: Float, times: Int): String = {
    val mul = 1 << times
    val xr = (x.toDouble * mul).toLong
    val yr = (y.toDouble * mul).toLong
    val zr = (z.toDouble * mul).toLong
    (times.toLong - 1).to(0L, -1L).map(t => {
      val mask = 1 << t
      (((xr & mask) >> t) |
        ((yr & mask) >> t) << 1 |
        ((zr & mask) >> t) << 2
        ).toByte
    }
    ).foldLeft("")(_ + _)
  }

  def createSon(father:String,p:Particle):String={
    val mul = 1 << father.length+1
    val xr = (p.x.toDouble * mul).toLong
    val yr = (p.y.toDouble * mul).toLong
    val zr = (p.z.toDouble * mul).toLong
    father + ((xr & 1) |((yr & 1)  << 1) | ((zr&1) << 2)).toByte

  }


}