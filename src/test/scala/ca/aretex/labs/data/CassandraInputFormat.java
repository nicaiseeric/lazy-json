package ca.aretex.labs.mr.hadoop;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import ca.aretex.labs.core.connector.cassandra.CassandraConnector;
import ca.aretex.labs.core.data.storage.Series;
import ca.aretex.labs.core.exception.CassandraException;
import ca.aretex.labs.mr.MRConfigArgument;
import ca.aretex.labs.mr.action.SeriesWorkflowUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class CassandraInputFormat extends InputFormat<LongWritable, DBInputSplit> {
	
	/* internal class logger */
	private static final Log LOGGER = LogFactory.getLog(CassandraInputFormat.class);
	
	/* current client id to process */
	private final String clientID;
	private final int maxMapper;

	/**
	 * Default empty constructor
	 */
	public CassandraInputFormat() {
		clientID = MRConfigArgument.CLIENT_ID.getValue();
		maxMapper = Integer.parseInt(MRConfigArgument.MAX_MAPPER_PER_CLIENT.getValue());
	}

	@Override
	public RecordReader<LongWritable, DBInputSplit> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new CassandraRecordReader((DBInputSplit) split);
	}

	private static List<List<Series>> partitionWithAtMost(List<Series> series, int maxPartition) {
		return Lists.partition(series, (series.size() / maxPartition) + 1);
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
	
		List<InputSplit> splits = new ArrayList<>();
		
		Stopwatch sw = new Stopwatch().start();
		
		int idx = 0;
		try {

			List<List<Series>> seriesLists = partitionWithAtMost(SeriesWorkflowUtil.getListSeriesShouldRun(clientID), maxMapper);
			for (List<Series> series : seriesLists) {
				DBInputSplit split = new DBInputSplit(idx++, series);
				splits.add(split);
			}
			
		} catch (CassandraException ex) {
			throw new IOException("Got CassandraException", ex);
		} finally {
			CassandraConnector.getInstance().close();
		}

		LOGGER.info(splits.size() + " series shouldRun (splits) found within " + sw.stop().elapsedTime(TimeUnit.MILLISECONDS) + "ms");
		return splits;
	
	}
	

}
