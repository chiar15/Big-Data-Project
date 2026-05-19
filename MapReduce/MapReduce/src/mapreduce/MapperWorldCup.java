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

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class for the World Cup MapReduce program.
 * This class extends the Mapper class from the Hadoop library.
 * It processes each line of input data and emits key-value pairs for analysis.
 */
public class MapperWorldCup extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    
    /**
     * The map method of the Mapper class.
     * Processes each line (record) of the dataset and emits key-value pairs.
     * Key: Team initials. Value: Count of 1 for each goal or penalty by a substitute.
     * 
     * @param key     The input key.
     * @param value   The input value (a line from the dataset).
     * @param context The context to write the output to.
     */
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Splitting the line into fields based on comma separator.
        String[] fields = value.toString().split(",", -1);
        
        // Extracting the team initials.
        String team = fields[2];
        
        // Extracting the events data.
        String events = fields[8];

        // Checking the line-up status ('N' for substitute).
        String line_up = fields[4];
        if (line_up.contentEquals("N")) {
            // Iterating through each character in the events string.
            for (int i = 0; i < events.length(); i++) {
                if (events.charAt(i) == 'G') {
                    // Ensure 'G' is not preceded by 'O' to avoid counting own goals ('OG').
                    if (i == 0 || events.charAt(i - 1) != 'O') {
                        // Writing a goal scored by a substitute player.
                        context.write(new Text(team), one);
                    }
                } else if (events.charAt(i) == 'P') {
                    // Ensure 'P' is not preceded by 'M' to avoid counting missed penalties.
                    if (i == 0 || events.charAt(i - 1) != 'M') {
                        // Writing a penalty scored by a substitute player.
                        context.write(new Text(team), one);
                    }
                }
            }
        }
    }
}
