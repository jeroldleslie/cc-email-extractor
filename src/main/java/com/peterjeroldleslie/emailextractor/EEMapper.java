package com.peterjeroldleslie.emailextractor;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.validator.routines.EmailValidator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.martinkl.warc.WARCWritable;

public class EEMapper extends MapReduceBase implements Mapper<LongWritable, WARCWritable, Text, Text> {
  private Text                outKey            = new Text();
  private Text                outValue          = new Text();
  private static final String CONVERSION_RECORD = "conversion";

  private static final String EMAIL_PATTERN     = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
      + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
  private Pattern             pattern;
  private Matcher             matcher;
  EmailValidator              validator         = EmailValidator.getInstance();

  public boolean validate(final String word) {
    try {
      if (validator.isValid(word)) {
        matcher = pattern.matcher(word);
        return matcher.matches();
      } else {
        return false;
      }
    } catch (Exception e) {
      return false;
    }

  }

  public void map(LongWritable key, WARCWritable value, OutputCollector<Text, Text> collector, Reporter reporter)
      throws IOException {
    pattern = Pattern.compile(EMAIL_PATTERN);
    String recordType = value.getRecord().getHeader().getRecordType();
    String targetURL = value.getRecord().getHeader().getTargetURI();
    String content = new String(value.getRecord().getContent());
    if (recordType.equals(CONVERSION_RECORD) && targetURL != null) {
      outValue.set(targetURL);
      // Normalize whitespace to single spaces.
      content = content.replaceAll("\\s+", " ");
      Set<String> emails = new HashSet<String>();
      for (String word : content.split(" ")) {
        if (validate(word)) {
          emails.add(word);
        }
      }
      for (String email : emails) {
        outKey.set(email);
        collector.collect(outKey, outValue);
      }
    }
  }
}
