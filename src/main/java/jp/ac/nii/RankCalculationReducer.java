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

import com.fasterxml.jackson.databind.ObjectMapper;

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

		Area area = new Area();
		area.setAreaName(keyIn.toString());

		int i = 0;
		for (Entry<String, Integer> entry : entries) {
			Rank rank = new Rank();
			rank.setCategory(entry.getKey());
			rank.setRestNum(entry.getValue());
			area.addRankList(rank);
			i++;
			// 上位3件でbreak
			if (i >= FilePathConstants.RANKING) {
				break;
			}
		}

		// json変換
		ObjectMapper om = new ObjectMapper();
		String jsonStr = om.writeValueAsString(area);

		// エリア名,業種1,業種2,業種3
		valueOut.set(jsonStr);

		context.write(nullWritable, valueOut);
	}

	public class ValueComparator implements Comparator<Map.Entry<String,Integer>> {
		@Override
		public int compare(Entry<String,Integer> entry1, Entry<String,Integer> entry2) {
			return entry2.getValue().compareTo(entry1.getValue());
		}
	}
}
