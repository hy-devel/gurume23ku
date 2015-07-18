package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * エリアごとの人気業種を計算するジョブのMapper
 * 以下のフォーマットのファイルが与えられる
 * エリア名,業種名
 */
public class PopularityCalculationMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);

	@Override
	public void map(LongWritable keyIn, Text valuein, Context context)
			throws IOException, InterruptedException {
		// エリア名と業種名のペア
		context.write(valuein, one);
	}
}
