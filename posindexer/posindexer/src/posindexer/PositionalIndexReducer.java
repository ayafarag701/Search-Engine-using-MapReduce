package posindexer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class PositionalIndexReducer extends Reducer<Text, Text, Text, Text> {
    private Text positions = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
       
        Map<String, List<String>> docPositionsMap = new TreeMap<>(new Comparator<String>() {
            @Override
            public int compare(String doc1, String doc2) {
                
                int num1 = Integer.parseInt(doc1.replaceAll("\\D", ""));
                int num2 = Integer.parseInt(doc2.replaceAll("\\D", ""));
                return Integer.compare(num1, num2);
            }
        });

  
        for (Text val : values) {
            String docInfo = val.toString(); 
            String[] docParts = docInfo.split(":");
            String docID = docParts[0];  
            String position = docParts[1];  

            if (!docPositionsMap.containsKey(docID)) {
            	docPositionsMap.put(docID, new ArrayList<String>());
            }
            docPositionsMap.get(docID).add(position);
        }

    
        StringBuilder positionList = new StringBuilder();
        for (Map.Entry<String, List<String>> entry : docPositionsMap.entrySet()) {
            String docID = entry.getKey();
            List<String> positionsList = entry.getValue();

            
            Collections.sort(positionsList);

            
            StringBuilder joinedPositions = new StringBuilder();
            for (String pos : positionsList) {
                if (joinedPositions.length() > 0) {
                    joinedPositions.append(", ");
                }
                joinedPositions.append(pos);
            }

            positionList.append(docID).append(": ");
            positionList.append(joinedPositions.toString()).append("; ");
        }

        
        positions.set(positionList.toString().trim());
        context.write(key, positions);
    }
}
