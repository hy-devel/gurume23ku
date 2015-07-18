package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * エリアごとの業種の出現数を計算するジョブのJobです。
 */
public class RestrauntCalculationJob extends Job {

	public RestrauntCalculationJob(String fileDir) throws IOException {
		this.setJobName("RestrauntCalculationJob");
		this.setJarByClass(RestrauntCalculationJob.class);

		// MapperクラスとReducerクラスを設定する
		this.setMapperClass(RestrauntCalculationMapper.class);
		this.setReducerClass(RestrauntCalculationReducer.class);

		// 中間データのKeyとValueの型を設定する
		this.setMapOutputKeyClass(Text.class);
		this.setMapOutputValueClass(Text.class);
		this.setOutputKeyClass(NullWritable.class);
		this.setOutputValueClass(Text.class);

		// 利用するInputFormatとOutputFormatを設定する
		this.setInputFormatClass(TextInputFormat.class);
		this.setOutputFormatClass(TextOutputFormat.class);

		// HDFS上の入力ファイルと出力ファイルのパスを設定するコードを記載してください
		Path inputFile = new Path(fileDir + "/" + FilePathConstants.DEFAULT_RESTRAUNT_FILE_NAME);
		Path outputFile = new Path(fileDir + "/" + FilePathConstants.AREA_CATEGORY_FILE_NAME);
		FileInputFormat.addInputPath(this, inputFile);
		FileOutputFormat.setOutputPath(this, outputFile);

		this.setNumReduceTasks(10);
	}
}
