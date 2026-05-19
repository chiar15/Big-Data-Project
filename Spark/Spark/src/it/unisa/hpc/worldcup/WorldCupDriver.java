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
 * Find the team (or teams, in case of ties) that has won the most matches with the fewest number of players on the field at the end of the game.
 */

package it.unisa.hpc.worldcup;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Comparator;
import scala.Serializable;

/**
 * Main driver class for the Spark application.
 * This class sets up the Spark context and processes the World Cup dataset.
 */
public class WorldCupDriver {

    // Custom comparator class for comparing Tuple2 objects based on their second element.
    private static class TupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {
        @Override
        public int compare(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) {
            return t1._2().compareTo(t2._2());
        }
    }

    public static void main(String[] args) {
        
        if (args.length != 2) {
            System.err.println("Usage: WorldCupDriver <input path> <output path>");
            System.exit(-1);
        }
        
        String inputPath = args[0]; 
        String outputPath = args[1];
        SparkConf conf = new SparkConf().setAppName("WorldCupAnalysis");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the dataset into an RDD.
        JavaRDD<String> lines = sc.textFile(inputPath);
        
        // Process the data to calculate goals and red cards for each team in each match.
        JavaPairRDD<String, Tuple2<Integer, Integer>> goalsAndCardsData = lines.mapToPair(line -> {
            String fields[] = line.split(",", -1);
            String match_id = fields[1];
            String team = fields[2];
            String events = fields[8];
            
            int goals = 0;
            int auto_goals = 0;
            int red_cards = (events.contains("SY") || events.contains("R")) ? 1 : 0;
            for (int i = 0; i < events.length(); i++) {
                if (events.charAt(i) == 'G' && (i == 0 || events.charAt(i - 1) != 'O')) {
                    goals += 1;
                } else if ((events.charAt(i) == 'O' || events.charAt(i) == 'W') && i > 0 && events.charAt(i - 1) == 'O') {
                    auto_goals += 1;
                } else if (events.charAt(i) == 'P' && (i == 0 || events.charAt(i - 1) != 'M')) {
                    goals += 1;
                }
            }
            
            return new Tuple2<>(match_id + "_" + team, new Tuple2<>(goals - auto_goals, red_cards));
        }).reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()));

        // Determine match winners based on the goals scored.
        JavaPairRDD<String, Tuple2<String, Tuple2<Integer, Integer>>> matchResultData = goalsAndCardsData.mapToPair(item -> {
            String parts[] = item._1().split("_");
            String match_id = parts[0];
            String team = parts[1];
            
            return new Tuple2<>(match_id, new Tuple2<>(team, new Tuple2<>(item._2()._1(), item._2()._2())));
        }).reduceByKey((a, b) -> {
            if (a._2()._1() > b._2()._1()) {
                return a;
            } else if (b._2()._1() > a._2()._1()) {
                return b;
            } else {
                return new Tuple2<>(null, new Tuple2<>(-1, -1));
            }
        });

        // Extract teams and their red card counts from match winners.
        JavaPairRDD<String, Integer> winnersCards = matchResultData.mapToPair(item -> {
            String team = item._2()._1();
            int cards = item._2()._2()._2();
            
            return new Tuple2<>(team, cards);
        });

        // Calculate the maximum number of red cards among the winning teams.
        JavaRDD<Integer> cards = winnersCards.values();
        int max_cards = cards.top(1).get(0);
        
        // Filter teams that won with the maximum red card count and count their wins.
        JavaPairRDD<String, Integer> teamWins = winnersCards.filter(item -> item._2() == max_cards)
            .mapToPair(item -> new Tuple2<>(item._1(), 1))
            .reduceByKey(Integer::sum);

        // Find the team with the most wins.
        TupleComparator tupleComparator = new TupleComparator();
        Tuple2<String, Integer> teamWithMostWins = teamWins.max(tupleComparator);
        
        int maxWins = teamWithMostWins._2();
        
        // Filter teams that have the same maximum number of wins.
        JavaPairRDD<String, Integer> teamsWithMaxWins = teamWins.filter(item -> item._2().equals(maxWins));

        // Save the result to the specified output path.
        teamsWithMaxWins.saveAsTextFile(outputPath);

        // Close the Spark context.
        sc.close();
    }
}
