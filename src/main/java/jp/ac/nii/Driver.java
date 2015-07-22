package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 実行ジョブ制御クラス
 * 実行時の引数に応じて、実行するHadoopジョブを切り替える
 */
public class Driver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {

		// ジョブの数が増えると、実行引数に応じて実行するジョブをここで制御する
		if (args.length == 0 || "all".equals(args[0])) {
			int returnCode = 0;

			returnCode |= runRestaurantCalculationJob(args);
			returnCode |= runPopularityCountJob(args);
			returnCode |= runRankCalculationJob(args);

			return returnCode;

		} else if ("rest".equals(args[0])) {
			return runRestaurantCalculationJob(args);

		} else if ("pop".equals(args[0])) {
			return runPopularityCountJob(args);

		} else if ("rank".equals(args[0])) {
			return runRankCalculationJob(args);
		}

		return -1;
	}

	public int runRestaurantCalculationJob(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Job RestaurantCalculationJob = new RestaurantCalculationJob();

		return (RestaurantCalculationJob.waitForCompletion(true)) ? 0 : 1;
	}

	public int runPopularityCountJob(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Job popularityCalculationJob = new PopularityCalculationJob();

		return (popularityCalculationJob.waitForCompletion(true)) ? 0 : 1;
	}

	public int runRankCalculationJob(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Job rankCalculationJob = new RankCalculationJob();

		return (rankCalculationJob.waitForCompletion(true)) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		int returnCode = ToolRunner.run(new Driver(), args);
		System.exit(returnCode);
	}
}
