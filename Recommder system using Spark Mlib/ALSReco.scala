
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation._
import org.apache.spark.mllib

object ALSReco {
  def main(args: Array[String]): Unit =
    {    

      //Creating a spark context       
      val conf = new SparkConf().setAppName("ALSRecoApps").setMaster("yarn");
      val sc = new SparkContext(conf);
      sc.setCheckpointDir("hdfs://CSC570BD-BE-Master:9000/user/hive/checkpoint/")

      //body of the  rank function for user  
            
      def getSingleUserRank(actSet: Iterable[(Int, Double)],predictSet:Iterable[(Int,Double)]):Double ={
      
       val actArr = actSet.toArray
       val predictArr = predictSet.toArray
       var perValue  = 0.0 ; 
       var aidRank = 0.0 
       var addActRate = 0.0
       
       
       for (i<- 0 to ((predictArr.size.toInt)-1))     // for given aid's 
       {
         for (j<- 0 to ((actArr.size.toInt)-1))
         {
           if(predictArr(i)._1 ==  actArr(j)._1)   // if aid is same 
           {
             // get index of predicted id and calc the percentile Value 
              var index = i+1
              perValue = index.toDouble / predictArr.size.toDouble
              aidRank = aidRank+(perValue * actArr(j)._2)   // actual rate * percentile rank add it other aid rank.
            
           }
           
         }
               
       }
        // add a actual ratings 
       for (i<- 0 to ((actArr.size.toInt)-1))
       {
          addActRate = addActRate + actArr(i)._2
       }
       
       var perUserRank = aidRank/addActRate
       return perUserRank
      }


      
      val data = sc.textFile("hdfs://CSC570BD-BE-Master:9000/user/hive/warehouse/profiledata_06-May-2005/user_sample_1.csv", 2)
      val ua = data.map(x => x.split("\\s+")).map(l => (l(1), (l(0), l(2)))) // aid,(uid,cnt)
      val info = sc.textFile("hdfs://CSC570BD-BE-Master:9000/user/hive/warehouse/profiledata_06-May-2005/artist_alias.txt", 2)
      val aa = info.map(x => x.split("\\s+")).filter(x => x(0) != "" && x(1) != "").map(x => (x(0), x(1))) // bid,goodid
      val joinUA = ua.leftOuterJoin(aa)
      // uid, aid, count
      val mapJoin = joinUA.map { case (key, (ucpair, gid)) => (((ucpair._1).toInt, (gid.getOrElse(key).toInt)), (ucpair._2).toDouble) }
      val reduMapJoin = mapJoin.reduceByKey((a, b) => (a + b))

      // create ratings file in required format

      val uaRDD = reduMapJoin.map { case ((uid, aid), count) => Rating(uid, aid, count) }
      //create test and train set
      val Array(testSet, trainSet) = uaRDD.randomSplit(Array(0.2, 0.8))

      // build model on train set
      val rank = 5; val numIter = 20; val lambda = 0.01; val alpha = 0.01;
      val model = ALS.trainImplicit(trainSet, rank, numIter,lambda,alpha)
    		  //refered mllib documentation example
        val usersProd = testSet.map {
        case Rating(uid, aid, count) =>
          (uid, aid)
      }

      val predictions =
        model.predict(usersProd).map {
          case Rating(uid, aid, count) =>
            (uid, (aid, count))
        }

      val actsset= testSet.map {
        case Rating(uid, aid, count) =>
          (uid,( aid, count))
      }.groupByKey()

      //sort predicitons set in descending order on count value and combine by key
     val p = predictions.sortBy(l=>l._2._2,false)  
      val predSet = p.groupByKey()
      
      val countAndsPreds =  actsset.join(predSet)
     
     // call the getSingleUserRankFunction 
      val allUserRank= countAndsPreds.map{case (userid, (actSet,predictSet) )=> getSingleUserRank(actSet,predictSet)}
      val overallRank = allUserRank.mean
      printf("\nOverall Rank  :" + overallRank)
//      Overall Rank  :0.3854051204146543
            sc.stop()
       
      

    }
}