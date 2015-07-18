package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * エリアごとの人気業種を計算するジョブのMapper
 * 以下のフォーマットのファイルが与えられる
 * エリア名,業種名
 */
public class RestrauntCalculationMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private Text keyOut = new Text();
	private Text valueOut = new Text();

	@Override
	public void map(LongWritable keyIn, Text valuein, Context context)
			throws IOException, InterruptedException {
		String[] values = valuein.toString().split(",");

		//　店舗コード
		keyOut.set(values[0]);

		// エリア名、業種名
		valueOut.set(values[1] + "," + values[2]);

		// エリア名と業種名のペア
		context.write(keyOut, valueOut);
	}
}
