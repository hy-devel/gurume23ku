package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 実行ジョブ制御クラス
 * 実行時の引数に応じて、実行するHadoopジョブを切り替える
 * 引数1（必須）：ファイルパス名
 * 引数2（任意）：実行ジョブ名
 */
public class Driver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {

		if (args.length == 0) {
			System.out.println("args[0] is mandatory.");
			return -1;
		}

		if (args.length == 1 || "all".equals(args[1])) {
			int returnCode = 0;

			returnCode |= runRestrauntCalculationJob(args);
			returnCode |= runPopularityCountJob(args);
			returnCode |= runRankCalculationJob(args);

			return returnCode;

		} else if ("rest".equals(args[1])) {
			return runRestrauntCalculationJob(args);

		} else if ("pop".equals(args[1])) {
			return runPopularityCountJob(args);

		} else if ("rank".equals(args[1])) {
			return runRankCalculationJob(args);
		}

		return -1;
	}

	public int runRestrauntCalculationJob(String[] args) throws IOException,
	ClassNotFoundException, InterruptedException {
		Job restrauntCalculationJob = new RestrauntCalculationJob(args[0]);

		return (restrauntCalculationJob.waitForCompletion(true)) ? 0 : 1;
	}

	public int runPopularityCountJob(String[] args) throws IOException,
	ClassNotFoundException, InterruptedException {
		Job popularityCalculationJob = new PopularityCalculationJob(args[0]);

		return (popularityCalculationJob.waitForCompletion(true)) ? 0 : 1;
	}

	public int runRankCalculationJob(String[] args) throws IOException,
	ClassNotFoundException, InterruptedException {
		Job rankCalculationJob = new RankCalculationJob(args[0]);

		return (rankCalculationJob.waitForCompletion(true)) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		int returnCode = ToolRunner.run(new Driver(), args);
		System.exit(returnCode);
	}
}
