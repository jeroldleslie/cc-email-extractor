package com.peterjeroldleslie.emailextractor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.martinkl.warc.mapred.WARCInputFormat;

public class EmailExtractor extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(EmailExtractor.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new EmailExtractor(), args);
    System.exit(res);
  }

  public int run(String[] arg0) throws Exception {
    JobConf conf = new JobConf(getConf(), EmailExtractor.class);
    conf.set("fs.s3n.awsAccessKeyId", arg0[0]);
    conf.set("fs.s3n.awsSecretAccessKey", arg0[1]);
    String inputPath = arg0[2];
    LOG.info("Input path: " + inputPath);
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    String outputPath = arg0[3];
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    conf.setInputFormat(WARCInputFormat.class);

    conf.setOutputFormat(TextOutputFormat.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(EEMapper.class);
    conf.setReducerClass(EEReducer.class);

    JobClient.runJob(conf);
    return 0;
  }

}
