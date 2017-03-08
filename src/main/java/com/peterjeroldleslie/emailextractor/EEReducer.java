package com.peterjeroldleslie.emailextractor;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class EEReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  private Text outValue = new Text("");

  public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> output, Reporter reporter)
      throws IOException {
    Set<String> links = new HashSet<String>();
    while (value.hasNext()) {
      links.add(value.next().toString());
    }
    
    for(String link:links){
      outValue.set(link);
      output.collect(key, outValue); 
    }
    
  }
}
