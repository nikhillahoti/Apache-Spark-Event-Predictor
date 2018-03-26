package project;

import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SQLContext;


public class Predictor {
   
    @SuppressWarnings("deprecation")
	public static void main(String[] args) {
   
    	// checking the number of parameters 
    	if (args.length < 2) {
            System.err.println("Usage: Predictor <filePath> <TeamName> <output-path>");
            System.err.println("eg: Predictor /user/user01/ginf.csv Barcelona /home/user01/puuu.txt ");
            System.exit(1);
        }

    	final Pattern SPACE = Pattern.compile("[\\s+\\t]");
    	String master = "local[*]";
      
      // Getting the team names for which the prediction is to be made, Output filepath and the input filepath
    	final String filepath = args[0].trim().toString();
    	final String Team = args[1].trim().toString();
    	final String outputPath = args[2].trim().toString();
    	System.out.println("Selected team is: "+Team);
    	
    	HashMap<String, Integer> form_pattern = new HashMap<>();
    	
      // initialize the streaming context
      JavaSparkContext jssc = new JavaSparkContext(master,"Predictor");
      SQLContext sqlContext = new SQLContext(jssc);
	
      // Creating a Records RDD for Matches
      JavaRDD<Events> rdd_records = jssc.textFile(filepath).map(
      new Function<String, Events>() {
			private static final long serialVersionUID = 1L;

			public Events call(String line) throws Exception {
                 String[] fields = line.split(",");
                 Events sd = new Events( fields[3].trim()
                		 				,fields[4].trim()
                		 				,fields[5].trim()
                		 				,fields[6].trim()
                		 				,fields[7].trim()
                		 				,fields[8].trim()
                		 				,fields[9].trim()
                		 				,fields[10].trim()
                		 				,fields[11].trim()
                		 				,fields[12].trim()
                		 				,fields[13].trim()                		 	
                		 );
                 return sd;
              }
        });        
        
      // Filter data only for a Specific club
      JavaRDD<Events> singleClubData = rdd_records.filter(new Function<Events, Boolean>() {
			private static final long serialVersionUID = 1L;
			public Boolean call(Events line) {

            	// Filter data which matches the team name.
              // Checking if either the home or the away team is the current team
            	if(line.ht.toUpperCase().trim().contains(Team.toUpperCase().trim()) || line.at.toUpperCase().trim().contains(Team.toUpperCase().trim())) {
            		if(line.ht.toUpperCase().trim().contains(Team.toUpperCase().trim())){
            			line.setIsAwayTeam(false);
            		}
            		else
            		{
            			line.setIsAwayTeam(true);
            		}
            		return true;
            	}else {
            		return false;
            	}
            	
        }});
        List<Events> singleclubb = singleClubData.collect();
        System.out.println(singleclubb.size());
        
        // get the data from 2012 to 2015 for training 
       JavaRDD<Events> singleClubData_2012_2015 = singleClubData.filter(new Function<Events, Boolean>() {
			private static final long serialVersionUID = 1L;
			public Boolean call(Events line) {
				
				if(line.date.contains("date"))
				{
					return true;
				}
				
				String[] fields = line.date.split("-");
				int year = Integer.parseInt(fields[0]);
				int month = Integer.parseInt(fields[1]);
            	if((year == 2016 && month < 6) || year < 2016)
            	{
            		return true;
            	}
            	else {
            		return false;
            	}
            	
        }});
       
       List<Events> singleclub = singleClubData_2012_2015.collect();
       String form_pattern_2012_2015 = "";
       for(final Events evt: singleclub) {
    	   //System.out.println("Home team: " + evt.date);
    	   form_pattern_2012_2015 = form_pattern_2012_2015 + evt.result;
       }
       System.out.println(" Form 2012 - mid-2016 : " + form_pattern_2012_2015);
       
       // Testing it on the data
       JavaRDD<Events> singleClubData_2016_2017 = singleClubData.filter(new Function<Events, Boolean>() {
			private static final long serialVersionUID = 1L;
			public Boolean call(Events line) {
				
				if(line.date.contains("date"))
				{
					return true;
				}
				
				String[] fields = line.date.split("-");
				int year = Integer.parseInt(fields[0]);
				int month = Integer.parseInt(fields[1]);
           	if((year == 2016 && month > 5) || year > 2016)
           	{
           		return true;
           	}
           	else 
           	{
           		return false;
           	}   	
       }});
       
       List<Events> singleclubs = singleClubData_2016_2017.collect();
       System.out.println(singleclubs.size());
       String form_pattern_finalSet = "";
       for(final Events evt: singleclubs) {
    	   // System.out.println("Home team: " + evt.ht + " Away Team:" + evt.at + " Result : " + evt.result + " Date : " + evt.date );
    	   form_pattern_finalSet = form_pattern_finalSet + evt.result;
       } 
       System.out.println(" form mid 2016 - 2017 : " + form_pattern_finalSet);
       
       FinalPredictor(form_pattern_2012_2015,form_pattern_finalSet);
       
       jssc.close();
    }
    
    public static void FinalPredictor(String FormSequence20122017, String FormSequence2017)
    {
    	int lastIndex = 0;
		int predIndex = 4;
		int predictioncount = 0;
		int correctpredictioncount = 0;

		while(lastIndex < FormSequence2017.length() - 3){
			
			String currSubString = FormSequence2017.substring(predIndex - 4, predIndex - 1);
			int internalCOunter = 0;
			String SelectedInternalString = "";
			int CurrentInternalMax = 0;
			while (internalCOunter < 3)
			{
				String[] valueadd = {"L","D","W"}; 
				String derivedSubString = currSubString + valueadd[internalCOunter]; 
				internalCOunter++;
				
				int internalCount = 0;
				int internalLastIndex = 0;
				while(internalLastIndex != -1){

					internalLastIndex = FormSequence20122017.indexOf(derivedSubString,internalLastIndex);

				    if(internalLastIndex != -1){
				    	internalCount ++;
				        internalLastIndex += 1;
				    }
				}
				
				if (internalCount > CurrentInternalMax) 
				{
					SelectedInternalString = derivedSubString;
				}
			//	System.out.println(" Count for Substring " + derivedSubString + " : " + internalCount);
			}
			String CurrentPrediction = FormSequence2017.substring(predIndex - 4, predIndex);
			System.out.println(" Our Prediction : " + SelectedInternalString);
			System.out.println(" Actual Prediction : " + CurrentPrediction);
			System.out.println(" ----------------------------------------------------- ");
			if ( CurrentPrediction.equals(SelectedInternalString)) 
			{
				correctpredictioncount ++;
			}
			predictioncount++;
			lastIndex++;
			predIndex++;
		}
		System.out.println(" Prediction Count : " + predictioncount);
		System.out.println(" Prediction Correct : " + correctpredictioncount);
    }
}
    
