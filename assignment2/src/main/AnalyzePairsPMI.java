
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class AnalyzePairsPMI {

    public static void main(String[] args) {
    
    	Answer ans = new Answer();
    	
    	System.out.println(ans.findTopTenFractions());
    	System.out.println(ans.findBigramsAppearOnlyOnce());
    	
    }
	
}

class ValueComparator implements Comparator<String> {
	
    Map<String, Integer> base;
    public ValueComparator(Map<String, Integer> base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.    
    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}

class Answer {
	
	private static final String INPUT_FILE = "C:/Users/user/UMD/Spring2013/MapReduce/Temp/part-r-00000";
	
	private HashMap<String, Integer> counts;
	
	public Answer() {
		try {
			getCounts();
			
			System.out.println(counts.size());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
    private void getCounts() throws IOException {
    	
    	counts = new HashMap<String, Integer>();
    	
        BufferedReader in = new BufferedReader(
	    	new FileReader(new File(INPUT_FILE))
		);
	    
	    String line = "", tokens[];
	    while ((line = in.readLine()) != null){
	    	tokens = line.split("\t");
	    	counts.put(tokens[0], Integer.parseInt(tokens[1]));
	    }
	    
	    in.close();
    }
    
    public String[] findTopTenBigrams() {
	    
		String[] result = new String[10];
		
	    // Sort the counts
	    TreeMap<String,Integer> sorted = new TreeMap<String,Integer>(new ValueComparator(counts));
	    sorted.putAll(counts);
	    
	    // print the top ten
	    String[] sortedKeys = sorted.keySet().toArray(new String[0]);
	    for (int ii = 0; ii < 10; ii++) {
	    	result[ii] = sortedKeys[ii];
	    	System.out.println(result[ii]);
	    }
	    
		return result;
	}
    
    public double findTopTenFractions() {
		int total = 0;
		int topten = 0;
		System.out.println(counts.size());
		Integer[] values = counts.values().toArray(new Integer[0]);
		
		for (int x : values) total += x;
		
		String[] toptens = findTopTenBigrams();
		for (String s : toptens) topten += counts.get(s);
		
		return (double)topten / (double)total;
		
	}
    
    public int findBigramsAppearOnlyOnce() {
    	
    	ArrayList<String> onceBigrams = new ArrayList<String>();
    	String[] keys = counts.keySet().toArray(new String[0]);
    	
    	for (String k : keys) {
    		if (counts.get(k) == 1)
    			onceBigrams.add(k);
    	}
    	
    	return onceBigrams.size();
    }
	
}