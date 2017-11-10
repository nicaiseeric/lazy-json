package ca.aretex.labs.mr.hadoop;

import ca.aretex.labs.core.data.storage.Series;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * A InputSplit that spans a set of rows
 */
@InterfaceStability.Evolving
public class DBInputSplit extends InputSplit implements Writable {
	private final LongWritable idx;
	private ArrayWritable clients_ids;
	private ArrayWritable series_ids;

	public DBInputSplit() {
		idx = new LongWritable();
		clients_ids = new ArrayWritable(new String[]{});
		series_ids = new ArrayWritable(new String[]{});
	}

	/**
	 * Default Constructor
	 */
	public DBInputSplit(long idx, List<Series> series) {
		this.idx = new LongWritable(idx);
		String[] clients = new String[series.size()];
		String[] seriess = new String[series.size()];

		int ix = 0;
		for(Series s: series) {
			clients[ix] = s.getClientId();
			seriess[ix] = s.getSeriesId();
			ix++;
		}
		clients_ids = new ArrayWritable(clients);
		series_ids = new ArrayWritable(seriess);
	}

	public long getIdx() {
		return idx.get();
	}

	public String[] getClientIds() {
		return clients_ids.toStrings();
	}

	public String[] getSeriesId() {
		return series_ids.toStrings();
	}


	public void write(DataOutput output) throws IOException {
		idx.write(output);
		clients_ids.write(output);
		series_ids.write(output);
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		idx.readFields(input);
		clients_ids.readFields(input);
		series_ids.readFields(input);
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return 1;
	}

	/** {@inheritDoc} */
	public String[] getLocations() throws IOException {
		return new String[] {};
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this)
				.append("idx", idx)
				.append("clientId", clients_ids.toStrings())
				.append("seriesId", series_ids.toStrings())
				.toString();
	}
}