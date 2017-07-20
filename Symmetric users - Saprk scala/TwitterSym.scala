import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object TwitterSym {
   def main(args: Array[String]): Unit = {
    if (args.length < 2){
        System.err.println("Usage: TwitterSymmetricUsers <input file>  <output file>")
        System.exit(1); 
        }
    
    //Creating a spark context
    val conf = new SparkConf().setAppName("TwitterSymmetricUsers").setMaster("yarn")
    val sc = new SparkContext(conf)
    val twitterdd=sc.textFile(args(0),2)
    val t1=twitterdd.map(line=>line.split(" "))
    val t2RA=t1.map(m=>(((m(0)+","+m(1))),((m(0)),(m(1)))))
    val t2RB=t1.map(m=>(((m(1)+","+m(0))),((m(1)),(m(0)))))
    val joinRab=t2RA.join(t2RB)
    
    val casereult=joinRab.filter{case(key,((a,b),(c,d)))=>(a.toInt<b.toInt)}
   
    val result = casereult.map{case(key,(a,b))=>key}
    result.saveAsTextFile(args(1));

    sc.stop();
      }
}