
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMul {

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

        Path inputPath = new Path("Input/M");
        Path outputDir = new Path("Output12");

        // Create configuration
        Configuration conf = new Configuration();
        conf.set("m", "3");
        conf.set("n", "3");
        conf.set("p", "3");
        @SuppressWarnings("deprecation")

        // Create job
        Job job = new Job(conf, "MatrixMul");
        job.setJarByClass(MatrixMul.class);

        // Setup MapReduce
        job.setMapperClass(MatrixMulMapper.class);
        job.setReducerClass(MatrixMulReducer.class);
        //job.setNumReduceTasks(1);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);

      /*  // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);
*/
        // Execute job
       // int code = job.waitForCompletion(true) ? 0 : 1;
        //System.exit(code);
        job.waitForCompletion(true);
    }

}
