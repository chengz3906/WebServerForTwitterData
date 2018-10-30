package cmu.cc.team.spongebob.etl.phase1.s3_to_hfiles;

import lombok.Cleanup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;


public class HBaseETLJob {
    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = "172.31.46.38";

    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf,"Phase1_HBase_ETL");
        job.setJarByClass(HBaseETLJob.class);

        // set s3 credentials
        job.getConfiguration().set("fs.s3n.awsAccessKeyId", System.getenv("S3_ACCESS_KEY"));
        job.getConfiguration().set("fs.s3n.awsSecretAccessKey", System.getenv("S3_SECRET_KEY"));
        conf.set("hbase.master", zkAddr + ":14000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        Connection testCon = ConnectionFactory.createConnection(conf);

        job.setMapperClass(BulkLoadMapper.class);
        job.setReducerClass(BulkLoadReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);

        FileInputFormat.setInputPaths(job, inputPath);
        HFileOutputFormat2.setOutputPath(job, new Path(outputPath));

        @Cleanup Connection hbCon = ConnectionFactory.createConnection(conf);
        @Cleanup Table hTable = hbCon.getTable(TableName.valueOf("tweet_intimacy"));
        @Cleanup RegionLocator regionLocator = hbCon.getRegionLocator(TableName.valueOf("tweet_intimacy"));
        HFileOutputFormat2.configureIncrementalLoad(job, hTable, regionLocator);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
