package jp.ac.nii;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * エリアごとの人気業種を計算するジョブのReducerです。
 */
public class RestaurantCalculationReducer extends
		Reducer<Text, Text, NullWritable, Text> {

	private static final NullWritable nullWritable = NullWritable.get();

	private Text valueOut = new Text();

	@Override
	public void reduce(Text keyIn, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> iterator = values.iterator();

		// 店舗Cdによる重複削除のため、最初のデータのみ出力する
		if (iterator.hasNext()) {
			valueOut.set(iterator.next().toString());
		}

		context.write(nullWritable, valueOut);
	}
}
