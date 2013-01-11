package org.apache.hbase.contrib.coprocessor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALObserver;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Keeps the index table up to date when changes happen to the primary table. It
 * does that by monitoring the WAL writing.
 */
public class SecondaryIndexUpdater extends BaseRegionObserver implements
		WALObserver {
	private static final Log LOG = LogFactory
			.getLog(SecondaryIndexUpdater.class);
	private static final String PIMARY_TABLE_NAME = "tableName";
	private static final String INDEX_TABLE_NAME = "idxTableName";
	private static final String INDEXED_FAMILY = "indexedFamily";
	private static final String INDEXED_QUALIFIER = "indexedQualifier";
	private static final byte[] IDX_FAMILY = Bytes.toBytes("I");
	private static final byte[] IDX_QUALIFIER = Bytes.toBytes("i");

	private byte[] primaryTableName;
	private byte[] indexTableName;
	private byte[] indexedFamily;
	private byte[] indexedQualifier;

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		this.primaryTableName = Bytes.toBytes(env.getConfiguration().get(
				PIMARY_TABLE_NAME));
		this.indexedFamily = Bytes.toBytes(env.getConfiguration().get(
				INDEXED_FAMILY));
		this.indexedQualifier = Bytes.toBytes(env.getConfiguration().get(
				INDEXED_QUALIFIER));
		this.indexTableName = Bytes.toBytes(env.getConfiguration().get(
				INDEX_TABLE_NAME));
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
		// nothing to do
	}

	@Override
	public void postWALWrite(ObserverContext<WALCoprocessorEnvironment> env,
			HRegionInfo info, HLogKey logKey, WALEdit logEdit)
			throws IOException {
		// check table name matches or not
		if (Arrays.equals(info.getTableName(), this.primaryTableName)) {
			List<KeyValue> kvs = logEdit.getKeyValues();
			for (KeyValue kv : kvs) {
				byte[] family = kv.getFamily();
				byte[] qualifier = kv.getQualifier();

				if (Arrays.equals(family, this.indexedFamily)
						&& Arrays.equals(qualifier, this.indexedQualifier)) {
					LOG.debug("Found KeyValue from WALEdit which should be indexed.");
					byte[] idxRow = createCompositeRowKey(kv.getValue(),
							kv.getRow());
					if (kv.isDelete() && kv.isDeleteType()) {
						System.out
								.println("delete cell ["
										+ Bytes.toString(kv.getRow()) + " ,"
										+ Bytes.toString(qualifier)
										+ "] with version ["
										+ kv.getTimestamp() + "]!");
						HTableInterface idxTable = env.getEnvironment()
								.getTable(indexTableName);
						// partial row scan because first part is unknown
						// ??_<state>
						ResultScanner scanner = findRowsWithParialKey(idxTable,
								kv.getRow());
						Delete del = null;
						for (Result res : scanner) {
							// same timestamp?
							if (res.raw()[0].getTimestamp() == kv
									.getTimestamp()) {
								del = new Delete(res.getRow(),
										kv.getTimestamp(), null);
								idxTable.delete(del);
							}
						}
						scanner.close();
					} else {
						System.out.println("write row ["
								+ Bytes.toString(idxRow) + "] with version ["
								+ kv.getTimestamp() + "]!");
						Put p = new Put(idxRow);
						p.add(IDX_FAMILY, IDX_QUALIFIER, kv.getTimestamp(),
								Bytes.toBytes(""));
						HTableInterface idxTable = env.getEnvironment()
								.getTable(indexTableName);
						idxTable.put(p);
						idxTable.flushCommits();
					}
				} else if (kv.isDelete()
						&& Arrays.equals(family, this.indexedFamily)
						&& kv.isEmptyColumn()) {
					System.out.println("delete entire row ["
							+ Bytes.toString(kv.getRow()) + "]");
					HTableInterface idxTable = env.getEnvironment().getTable(
							indexTableName);
					// partial row scan because first part is unknown ??_<state>
					ResultScanner scanner = findRowsWithParialKey(idxTable,
							kv.getRow());
					Delete del = null;
					for (Result res : scanner) {
						del = new Delete(res.getRow());
						idxTable.delete(del);
					}
					scanner.close();
				}
			}
		}

	}

	@Override
	public boolean preWALWrite(ObserverContext<WALCoprocessorEnvironment> env,
			HRegionInfo info, HLogKey logKey, WALEdit logEdit)
			throws IOException {
		return false;
	}

	/**
	 * Triggered after {@link org.apache.hadoop.hbase.regionserver.HRegion} when
	 * WAL is restored.
	 */
	@Override
	public void postWALRestore(
			ObserverContext<RegionCoprocessorEnvironment> env,
			HRegionInfo info, HLogKey logKey, WALEdit logEdit)
			throws IOException {
	}

	@SuppressWarnings("unchecked")
	private ResultScanner findRowsWithParialKey(HTableInterface ht, byte[] row)
			throws IOException {
		FuzzyRowFilter rowFilter = new FuzzyRowFilter(
				Arrays.asList(new Pair<byte[], byte[]>(createCompositeRowKey(
						Bytes.toBytesBinary("\\x00\\x00"), row), new byte[] {
						1, 1, 0, 0, 0 })));
		Scan scan = new Scan();
		scan.addColumn(IDX_FAMILY, IDX_QUALIFIER);
		scan.setFilter(rowFilter);
		return ht.getScanner(scan);
	}

	/**
	 * Create composite row key by concatenating given byte[]. Delimiter between
	 * the byte[] is an underscore.
	 * 
	 * @param columnValue
	 * @param row
	 * @return the composite rowkey {columnValue}_{row}
	 */
	private byte[] createCompositeRowKey(byte[] columnValue, byte[] row) {
		return Bytes.add(columnValue, Bytes.toBytes("_"), row);
	}

	private String printCellCoordinates(HTableInterface ht, byte[] family,
			byte[] qualifier, long ts, byte[] value) {
		StringBuilder buffer = new StringBuilder();
		buffer.append(Bytes.toString(ht.getTableName()));
		buffer.append(", ");
		buffer.append(Bytes.toString(family));
		buffer.append(", ");
		buffer.append(Bytes.toString(qualifier));
		buffer.append(", ");
		buffer.append(ts);
		return buffer.toString();
	}

}