import java.io.File;
import java.io.IOException;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

public class EvaluateRecommender
{
    public static void main( String[] args )throws IOException, TasteException
    {
    	
    	DataModel model = new FileDataModel(new File(args[0]));
    	RecommenderBuilder recBuilder = new myRecommendSys();
    	
    	
    	RecommenderEvaluator evalMAE = new AverageAbsoluteDifferenceRecommenderEvaluator();
    	RecommenderEvaluator evalRMS =  new RMSRecommenderEvaluator();
    	
    	double MAEerrorrate = evalMAE.evaluate(recBuilder, null, model, 0.9, 1.0);
    	System.out.println("Mean Abslolute Error  : "+ MAEerrorrate);
      
    	double RMSerrorRate = evalRMS.evaluate(recBuilder, null, model, 0.9, 1.0);
    	System.out.println("RMSE : "+RMSerrorRate);
    	
        RecommenderIRStatsEvaluator irEval = new  GenericRecommenderIRStatsEvaluator();
    	IRStatistics irStat = irEval.evaluate(recBuilder, null, model,null,10,GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD,1.0);
    	
    	System.out.println("PRECISION : "+ irStat.getPrecision());
    	System.out.println("RECALL : "+ irStat.getRecall());
    }
}  
    
    class myRecommendSys implements RecommenderBuilder
    {
    	public Recommender buildRecommender(DataModel model)throws TasteException
    	{
    		UserSimilarity pSimilarity = new PearsonCorrelationSimilarity(model);
    		UserSimilarity  logLikely =  new LogLikelihoodSimilarity(model);
    		UserSimilarity eucliDist = new EuclideanDistanceSimilarity(model);
    					
    		NearestNUserNeighborhood nuser = new NearestNUserNeighborhood(150, pSimilarity, model);
    		UserBasedRecommender recommendSys = new GenericUserBasedRecommender(model, nuser, pSimilarity);
		
    		return recommendSys;
    		
    	}
    	
    	
    }




