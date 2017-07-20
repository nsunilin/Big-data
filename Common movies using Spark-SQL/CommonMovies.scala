import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object CommonMovies {
 
  def main(args: Array[String]): Unit ={
   if(args.length<3){
     System.err.println("Usage : CommonMovies <input file> <output file>")
     System.exit(1);
   }
     
    val spark=SparkSession.builder().master("yarn").appName("CommonMovies").
    config("spark.sql.warehouse.dir","hdfs://CSC570BD-BE-Master:9000/user/hive/warehouse").enableHiveSupport().
    getOrCreate()
   
    val sc = spark.sparkContext;
   
   
    import spark.implicits._
   
    //val movieDD=sc.textFile("/hadoop-user/data/movies",2)
    val movieDD=sc.textFile(args(0),2)
    case class Movies(movie_id:String,movie_title:String)
   
    val moviePair=movieDD.map(m=>m.split("#")).map(m=>Movies(m(0),m(1)))
    val movie=moviePair.toDF
   
    //val rate=sc.textFile("/hadoop-user/data/ratings.dat",2)
    val rate=sc.textFile(args(1),2)
    val ratePair=rate.map(x=>x.split("\\s+")).map(x=>(x(0),x(1))).flatMapValues(x=>x.split("#"))
    val userRating=ratePair.toDF
   
    val names=Map("_1"->"user_id","_2"->"movie_id")
    
    val userTable=userRating.select(userRating.columns.map(c=>col(c).as(names.getOrElse(c,c))):_*)
    
    val userR=userTable.createOrReplaceTempView("userR")
   
    val movieTable=movie.createOrReplaceTempView("movie")
   
    val res=spark.sql("select t1.user_id,t2.user_id,COUNT(t1.movie_id) as mcount,collect_set(mo.movie_title) FROM userR t1,userR t2, movie mo where t1.movie_id =t2.movie_id AND t1.user_id!=t2.user_id AND mo.movie_id=t1.movie_id AND t1.user_id < t2.user_id GROUP BY t1.user_id,t2.user_id having mcount >50")
    
    //res.rdd.saveAsTextFile("/hadoop-user/OUTPUT_Movie")        
    res.rdd.saveAsTextFile(args(2))
    sc.stop();
  }
}
