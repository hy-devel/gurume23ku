package jp.ac.nii;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * エリアごとの人気業種を計算するジョブのReducerです。
 */
public class PopularityCalculationReducer extends
		Reducer<Text, IntWritable, NullWritable, Text> {

	private static final NullWritable nullWritable = NullWritable.get();

	private Text valueOut = new Text();

	@Override
	public void reduce(Text keyIn, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		Iterator<IntWritable> iterator = values.iterator();

		while (iterator.hasNext()) {
			sum += iterator.next().get();
		}

		// エリア名,業種名,出現回数
		valueOut.set(keyIn.toString() + "," + sum);

		context.write(nullWritable, valueOut);
	}
}
