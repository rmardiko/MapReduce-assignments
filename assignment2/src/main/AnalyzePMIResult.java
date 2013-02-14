
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import edu.umd.cloud9.io.pair.PairOfStrings;

public class AnalyzePMIResult {

    public static void main(String[] args) {
    
    	Answer ans = new Answer();
    	
    	System.out.println(ans.findTheHighestPairPMI());
    	
    	ans.findHighestWordWith("cloud", 3);
    	
    	ans.findHighestWordWith("love", 3);
    }
	
}

class ValueComparator implements Comparator<PairOfStrings> {
  
  Map<PairOfStrings, Float> base;
  public ValueComparator(Map<PairOfStrings, Float> base) {
      this.base = base;
  }

  // Note: this comparator imposes orderings that are inconsistent with equals.    
  public int compare(PairOfStrings a, PairOfStrings b) {
      if (base.get(a) >= base.get(b)) {
          return -1;
      } else {
          return 1;
      } // returning 0 would merge keys
  }
}

class Answer {
	
	private static final String INPUT_FILE = "C:/Users/user/UMD/Spring2013/MapReduce/Temp/pairs/pairs_pmi_result";
	
	private HashMap<PairOfStrings, Float> pmis;
	
	public Answer() {
		try {
			getCounts();
			
			System.out.println(pmis.size());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
    private void getCounts() throws IOException {
    	
      pmis = new HashMap<PairOfStrings, Float>();
    	
      BufferedReader in = new BufferedReader(
	    	   new FileReader(new File(INPUT_FILE))
		  );
	    
	    String line = "", tokens[];
	    while ((line = in.readLine()) != null){
	    	
	      tokens = line.split("\\s+");
	    	
	    	String left = tokens[0].substring(1, tokens[0].length()-1);
	      String right = tokens[1].substring(0, tokens[1].length()-1);
	    	
	    	pmis.put(new PairOfStrings(left,right), Float.parseFloat(tokens[2]));
	    }
	    
	    in.close();
    }
    
    public PairOfStrings findTheHighestPairPMI() {
      
      float max = Float.MIN_VALUE;
      PairOfStrings highest = new PairOfStrings();
      
      for (Map.Entry<PairOfStrings, Float> entry : pmis.entrySet()) {
        if (entry.getValue() > max) {
          max = entry.getValue();
          
          highest = entry.getKey();
        }
      }
      
      return highest;
    }
    
    public PairOfStrings[] findHighestWordWith(String word, int top) {
      
      HashMap<PairOfStrings, Float> subsetOfWord = new HashMap<PairOfStrings, Float>();
      for (Map.Entry<PairOfStrings, Float> entry : pmis.entrySet()) {
        if (entry.getKey().getLeftElement().equals(word)) {
          subsetOfWord.put(entry.getKey(), entry.getValue());
        }
      }
      
      PairOfStrings[] result = new PairOfStrings[top];
      
      // Sort the counts
      TreeMap<PairOfStrings,Float> sorted = new TreeMap<PairOfStrings,Float>(new ValueComparator(subsetOfWord));
      sorted.putAll(subsetOfWord);
      
      // print the top ten
      PairOfStrings[] sortedKeys = sorted.keySet().toArray(new PairOfStrings[0]);
      for (int ii = 0; ii < top; ii++) {
        result[ii] = sortedKeys[ii];
        System.out.println(result[ii] + " " + subsetOfWord.get(result[ii]));
      }
      
      return result;
      
    }
	
}
