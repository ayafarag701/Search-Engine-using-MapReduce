package posindexer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PositionalIndexDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: PositionalIndex <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("docID", args[0]);

        Job job = Job.getInstance(conf, "Positional Index");
        job.setJarByClass(PositionalIndexDriver.class);
        job.setMapperClass(PositionalIndexMapper.class);
        job.setReducerClass(PositionalIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
