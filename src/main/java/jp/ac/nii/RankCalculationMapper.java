package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * エリアごとの業種のランキングを計算するジョブのMapper
 * 以下のフォーマットのファイルが与えられる
 * エリア名,業種名,出現数
 */
public class RankCalculationMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private Text keyOut = new Text();
	private Text valueOut = new Text();

	@Override
	public void map(LongWritable keyIn, Text valuein, Context context)
			throws IOException, InterruptedException {
		String[] values = valuein.toString().split(",");

		// エリアをキーに設定
		keyOut.set(values[0]);

		// 業種,出現数をvalueに設定
		valueOut.set(values[1] + "," + values[2]);

		// エリア名と業種名のペア
		context.write(keyOut, valueOut);
	}
}
