/*
 * Course: High Performance Computing 2023/2024
 *
 * Channel A-H
 *
 * Lecturer: D'Aniello Giuseppe   gidaniello@unisa.it
 * 
 * Student: Ferraioli Chiara      0622702169    c.ferraioli30@studenti.unisa.it
 * 
 *                  EXERCISE 1 - MapReduce
 * Create a Hadoop MapReduce program that analyzes the available dataset in a different way from the second exercise (e.g., a statistic, ranking, index, etc.).
 * The chosen statistic is to calculate the number of goals scored by substitute (bench) players for each nation. 
 */

package mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Driver class for the World Cup MapReduce program.
 * This class sets up the job configuration and controls the MapReduce job flow.
 */
public class DriverWorldCup {

    /**
     * Main method to set up and start the MapReduce job.
     * 
     * @param args Command line arguments, expecting two: input and output paths.
     */
    public static void main(String[] args) throws Exception {
        // Verifying that two arguments (input and output paths) are passed.
        if (args.length != 2) {
            System.err.println("Usage: DriverWorldCup <input path> <output path>");
            System.exit(-1);
        }
        
        // Setting up input and output paths from the command line arguments.
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        
        // Creating a new Hadoop configuration.
        Configuration conf = new Configuration();
        
        // Setting up a new job instance for this configuration.
        Job job = Job.getInstance(conf);

        // Naming the job for identification purposes.
        job.setJobName("WorldCup");
        
        // Setting the input and output formats for the job.
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Specifying the jar file that contains the driver, mapper, and reducer classes.
        job.setJarByClass(DriverWorldCup.class);

        // Setting the input and output format classes.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Setting the mapper and reducer classes for the job.
        job.setMapperClass(MapperWorldCup.class);
        job.setReducerClass(ReducerWorldCup.class);

        // Setting the output key and value classes for the mapper and reducer.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Starting the MapReduce job and waiting for its completion.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
