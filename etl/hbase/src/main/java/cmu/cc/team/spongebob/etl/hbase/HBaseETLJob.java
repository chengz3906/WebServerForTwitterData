package cmu.cc.team.spongebob.etl.phase1.s3_to_hfiles;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.PutCombiner;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class HBaseETLJob extends Configured implements Tool {
    private static final Log LOGGER = LogFactory.getLog(HBaseETLJob.class);

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(HBaseConfiguration.create(), new HBaseETLJob(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = null;
        try (Connection connection = ConnectionFactory.createConnection(getConf());
             Admin admin = connection.getAdmin()) {
            Path inputPath = new Path(args[0]);
            Path hfileOutputPath = new Path(args[1]);
            TableName tableName = TableName.valueOf(args[2]);

            job = Job.getInstance(getConf(), "Phase1_HBase_ETL");
            job.setJarByClass(HBaseETLJob.class);

            job.getConfiguration().set("fs.s3n.awsAccessKeyId", System.getenv("S3_ACCESS_KEY"));
            job.getConfiguration().set("fs.s3n.awsSecretAccessKey", System.getenv("S3_SECRET_KEY"));

            job.setMapperClass(ContactTweetMapper.class);

            FileInputFormat.setInputPaths(job, inputPath);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);

            createTable(admin, tableName);

            try (Table table = connection.getTable(tableName);
                RegionLocator regionLocator = connection.getRegionLocator(tableName)) {
                job.setMapOutputValueClass(Put.class);
                job.setCombinerClass(PutCombiner.class);
                job.setReducerClass(PutSortReducer.class);

                FileOutputFormat.setOutputPath(job, hfileOutputPath);
                HFileOutputFormat2.configureIncrementalLoad(job,
                        table.getTableDescriptor(),
                        regionLocator);
            }

            TableMapReduceUtil.addDependencyJars(job);
        }

        LOGGER.info("HBase ETL Job started...");

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static void createTable(Admin admin, TableName tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            LOGGER.warn("htable already exists, deleting htable...");
            try {
                admin.disableTable(tableName);
            } catch (TableNotEnabledException e) {
                // do nothing
            }
            admin.deleteTable(tableName);
        }

        HTableDescriptor htd = new HTableDescriptor(tableName);
        htd.addFamily(new HColumnDescriptor(Bytes.toBytes("user2")));
        htd.addFamily(new HColumnDescriptor(Bytes.toBytes("tweet")));
        LOGGER.info("creating htable...");
        admin.createTable(htd);
    }
}
