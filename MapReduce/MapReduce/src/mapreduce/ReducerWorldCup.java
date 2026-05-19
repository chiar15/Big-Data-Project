/*
 * Course: High Performance Computing 2023/2024
 *
 * Channel A-H
 *
 * Lecturer: D'Aniello Giuseppe    gidaniello@unisa.it
 * 
 * Student: Ferraioli Chiara       0622702169    c.ferraioli30@studenti.unisa.it
 * 
 *                  EXERCISE 1 - MapReduce
 * Create a Hadoop MapReduce program that analyzes the available dataset in a different way from the second exercise (e.g., a statistic, ranking, index, etc.).
 * The chosen statistic is to calculate the number of goals scored by substitute (bench) players for each nation. 
 * 
 */

package mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer class for the World Cup MapReduce program.
 * This class extends the Reducer class from the Hadoop library.
 * It processes input data from the Mapper and produces the final output.
 */
public class ReducerWorldCup extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    /**
     * The reduce method of the Reducer class.
     * It sums up the counts of goals scored by substitute players for each team.
     * 
     * @param key     The team initials.
     * @param values  An iterable collection of IntWritable counts.
     * @param context The context to write the output to.
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        
        int sum = 0;
        // Iterating through all values associated with a particular key and summing them.
        for (IntWritable val : values) {
          sum += val.get();
        }
        // Setting the total sum as the output value.
        result.set(sum);
        // Writing the key (team initials) and result (total goals by substitutes) to the context.
        context.write(key, result);
    }
}
