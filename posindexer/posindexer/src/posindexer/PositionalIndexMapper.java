package posindexer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import java.util.HashMap;
import java.util.Map;

public class PositionalIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text term = new Text();
    private Text positionInfo = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        String docID = fileName;  

     
        String[] lines = value.toString().split("\n");

        
        for (int i = 0; i < lines.length; i++) {
            String[] words = lines[i].split("\\s+");  
            for (int j = 0; j < words.length; j++) {
                term.set(words[j].toLowerCase());  
                positionInfo.set(docID + ":" + (j + 1));  
                context.write(term, positionInfo);  
            }
        }
    }
}
