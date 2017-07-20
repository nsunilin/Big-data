import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph

object TwittGraph {
def main(args: Array[String]): Unit = {
    if (args.length < 2){
        System.err.println("Usage: Graph <input file>  <output file>")
        System.exit(1); 
        }
    
    //Creating a spark context
    val conf = new SparkConf().setAppName("Graph").setMaster("local[2]")
    val sc = new SparkContext(conf) 
    
    val twitterdd=sc.textFile(args(0),2)
    //pairing the rdd in userid1,userid2 and converting it long for the nodes to calculate
    val getEdges = twitterdd.map(line=>line.split(" ")).map( x => ((x(0).toLong),(x(1).toLong)))
   //creating graph rdd as for edges
    val graph = Graph.fromEdgeTuples(getEdges,null)
    
    //getting indegree and the result is (vertex,indegree)
    val indegNodes= graph.inDegrees
    //nuber of nodes in graph
    val nodes = graph.numVertices
    
    //interchanging values to calculate probabilty distribution
    val interchange=indegNodes.map(m=>(m._2,m._1))
    
    val countIndegree=interchange.countByKey()
    //calculating the probabilty distribution for given key(indegree)
    val result=countIndegree.map(m=>(m._1,(m._2.toDouble/nodes.toDouble)))
    
    sc.parallelize(result.toSeq).saveAsTextFile(args(1))
    sc.stop();
      }

}
