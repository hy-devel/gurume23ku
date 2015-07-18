package jp.ac.nii;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * エリアごとの業種ランクを計算するジョブのReducerです。
 */
public class RankCalculationReducer extends
		Reducer<Text, Text, NullWritable, Text> {

	private static final NullWritable nullWritable = NullWritable.get();

	private Text valueOut = new Text();

	@Override
	public void reduce(Text keyIn, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Iterator<Text> iterator = values.iterator();

		HashMap<String, Integer> map = new HashMap<String, Integer>();

		while (iterator.hasNext()) {
			String[] vals = iterator.next().toString().split(",");
			map.put(vals[0], Integer.valueOf(vals[1]));
		}

		// value でソート
		List<Map.Entry<String,Integer>> entries = new ArrayList<Map.Entry<String,Integer>>(map.entrySet());
		Collections.sort(entries, new ValueComparator());

		String outStr = keyIn.toString();
		int rank = 0;
		for (Entry<String, Integer> entry : entries) {
			outStr += "," + entry.getKey() + "," + entry.getValue();
			rank++;
			// 上位3件でbreak
			if (rank >= FilePathConstants.RANKING) {
				break;
			}
		}

		// エリア名,業種1,業種2,業種3
		valueOut.set(outStr);

		context.write(nullWritable, valueOut);
	}

	public class ValueComparator implements Comparator<Map.Entry<String,Integer>> {
		@Override
		public int compare(Entry<String,Integer> entry1, Entry<String,Integer> entry2) {
			return ((Integer)entry2.getValue()).compareTo((Integer)entry1.getValue());
		}
	}
}
