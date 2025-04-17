import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * Hadoop MapReduce program to calculate the average interest rate by loan grade.
 */
public class InterestRateByGradeDriver {

    /**
     * Mapper class to extract grade and interest rate from the input dataset.
     */
    public static class InterestRateMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text grade = new Text();
        private DoubleWritable interestRate = new DoubleWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(","); // Assuming CSV format
            try {
                // Skip header row (first field is "id")
                if (fields.length > 0 && fields[0].trim().equals("id")) {
                    return; // Skip header
                }

                if (fields.length > 11) { // Ensure required columns exist
                    String loanGrade = fields[6].trim(); // Extract grade (column 7, 0-based index 6)
                    String intRateStr = fields[10].trim(); // Extract int_rate (column 11, 0-based index 10)

                    // Remove quotes if present (e.g., "7.97" -> 7.97)
                    intRateStr = intRateStr.replaceAll("\"", "");

                    // Parse interest rate
                    double intRate = Double.parseDouble(intRateStr);

                    // Process only if grade is not empty and int_rate is valid
                    if (!loanGrade.isEmpty()) {
                        grade.set(loanGrade);
                        interestRate.set(intRate);
                        context.write(grade, interestRate); // Emit (grade, int_rate)
                    } else {
                        System.err.println("Skipping record due to empty grade: " + value.toString());
                    }
                } else {
                    System.err.println("Skipping record due to insufficient columns: " + value.toString());
                }
            } catch (NumberFormatException e) {
                System.err.println("Skipping record due to invalid int_rate format: " + value.toString());
            } catch (ArrayIndexOutOfBoundsException e) {
                System.err.println("Skipping record due to array index error: " + value.toString());
            }
        }
    }

    /**
     * Reducer class to compute the average interest rate for each grade.
     */
    public static class InterestRateReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;

            // Sum up interest rates and count the number of loans for each grade
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            // Calculate average interest rate
            double average = (count > 0) ? sum / count : 0.0;
            result.set(average);
            context.write(key, result); // Emit (grade, average int_rate)
        }
    }

    /**
     * Driver class to configure and run the Hadoop MapReduce job.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Interest Rate by Grade");

        // Set the jar, mapper, and reducer classes
        job.setJarByClass(InterestRateByGradeDriver.class);
        job.setMapperClass(InterestRateMapper.class);
        job.setReducerClass(InterestRateReducer.class);

        // Set output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Set input and output paths from command-line arguments
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run the job and exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
