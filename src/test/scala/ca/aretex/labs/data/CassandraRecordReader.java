package ca.aretex.labs.mr.hadoop;

import ca.aretex.labs.core.connector.cassandra.CassandraConnector;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Created by Nelson Levert on 1/21/16.

 * A RecordReader that reads records from a SQL table.
 * Emits LongWritables containing the record number as
 * key and DBWritables as value.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CassandraRecordReader extends RecordReader<LongWritable, DBInputSplit> {

	private DBInputSplit split;
	private long pos = 0;
	private LongWritable key = null;
	private DBInputSplit value = null;

	/**
	 * @param split The InputSplit to read data for
	 */
	public CassandraRecordReader(DBInputSplit split) {
		this.split = split;
	}

	/** {@inheritDoc} */
	public void close() throws IOException {
		CassandraConnector.getInstance().close();
	}

	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		//do nothing
	}

	/** {@inheritDoc} */
	public LongWritable getCurrentKey() {
		return key;
	}

	/** {@inheritDoc} */
	public DBInputSplit getCurrentValue() {
		return value;
	}

	/** {@inheritDoc} */
	public float getProgress() throws IOException {
		return pos / (float)1;
	}

	/** {@inheritDoc} */
	public boolean nextKeyValue() throws IOException {
		if (pos == 0) {
			key = new LongWritable(split.getIdx());
			value = split;
			pos++;
			return true;
		} else {
			return false;
		}
	}
}

