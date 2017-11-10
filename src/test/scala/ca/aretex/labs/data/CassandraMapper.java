package ca.aretex.labs.mr.hadoop;

import ca.aretex.labs.core.AstrologConfig;
import ca.aretex.labs.core.dao.CassandraSeriesDAO;
import ca.aretex.labs.core.data.storage.Series;
import ca.aretex.labs.mr.MRConfigArgument;
import ca.aretex.labs.mr.action.SeriesWorkflowUtil;
import ca.aretex.labs.mr.logger.CassandraAppender;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.LogManager;

import java.io.IOException;
import java.util.Map;


public class CassandraMapper extends Mapper<LongWritable, DBInputSplit, IntWritable, Text> {

	/* internal class logger */
	private static final Log LOGGER = LogFactory.getLog(CassandraMapper.class);
	
	/* context series index separator to split client id and series id */
	private static final String CONTEXT_SERIES_INDEX_SEPARATOR = "__"; 
	

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		for (Map.Entry<String, String> e : context.getConfiguration()) {
			if (e.getKey().startsWith(AstrologConfig.ASTROLOG_CONFIG_PREFIX)) {
				String key = StringUtils.removeStart(e.getKey(), AstrologConfig.ASTROLOG_CONFIG_PREFIX);
				AstrologConfig.getInstance().set(key, e.getValue());
			}
		}
	}

	protected void runSeries(String clientId, String seriesId)  throws IOException, InterruptedException {
		Series series = CassandraSeriesDAO.getInstance().get(clientId, seriesId);

		if (series == null) {
			LOGGER.warn("Series has been deleted between the initial call to CassandraInputFormat and now, mapper aborted");
			return;
		}

		// stores logs into Cassandra
		CassandraAppender app = new CassandraAppender(clientId, seriesId);
		LogManager.getLoggerRepository().getRootLogger().addAppender(app);

		// TODO: should be removed !
		MRConfigArgument.CLIENT_ID.setValue(clientId);
		MRConfigArgument.SERIES_ID.setValue(seriesId);

		try {
			SeriesWorkflowUtil.runSeriesActions(series);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			app.close();
			LogManager.getLoggerRepository().getRootLogger().removeAppender(app);
		}

	}
	@Override
	protected void map(LongWritable key, DBInputSplit value, Context context) throws IOException, InterruptedException {

		String[] clients = value.getClientIds();
		String[] series = value.getSeriesId();
		for(int ix = 0; ix < series.length; ix++) {
			runSeries(clients[ix], series[ix]);
		}

	}
	
	/**
	 * Gets the context series index to identify the series by client
	 * @param series
	 * @return
	 */
	private static String getContextSeriesIndex(Series series) {
		return series.getClientId() + CONTEXT_SERIES_INDEX_SEPARATOR + series.getSeriesId();
	}
}
