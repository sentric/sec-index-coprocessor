package org.apache.hbase.contrib.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.TestWALObserver;
import org.apache.hadoop.hbase.coprocessor.WALObserver;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Class for testing the {@link WALObserver} coprocessor.
 */
public class TestSecondaryIndexUpdater {
	private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
	public static final byte[] INDEX_TABLE = Bytes.toBytes("index");
	public static final byte[] INDEX_FAMILY = Bytes.toBytes("I");
	public static final byte[] INDEX_QUALIFIER = Bytes.toBytes("i");
	public static final byte[] USER_TABLE = Bytes.toBytes("user");
	private static byte[][] USER_FAMILY = { Bytes.toBytes("u"),
			Bytes.toBytes("fam2"), Bytes.toBytes("fam3") };
	private static byte[][] USER_QUALIFIER = { Bytes.toBytes("firstname"),
			Bytes.toBytes("lastname"), Bytes.toBytes("state") };

	private Configuration conf;
	private FileSystem fs;
	private Path dir;
	private Path hbaseRootDir;
	private Path oldLogDir;
	private static HTable indexTable;

	@BeforeClass
	public static void setupBeforeClass() throws Exception {
		Configuration conf = TEST_UTIL.getConfiguration();
		conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
				SecondaryIndexUpdater.class.getName());
		conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
				"org.apache.hadoop.hbase.coprocessor.AggregateImplementation");
		conf.setBoolean("dfs.support.append", true);
		conf.set("tableName", "user");
		conf.set("idxTableName", "index");
		conf.set("indexedFamily", "u");
		conf.set("indexedQualifier", "state");

		long startInNsec = System.nanoTime();
		TEST_UTIL.startMiniCluster(1);
		indexTable = TEST_UTIL.createTable(INDEX_TABLE, Bytes.toBytes("I"));
		long upInNsec = System.nanoTime();
		float delta = (float) (upInNsec - startInNsec) / 1000000000;
		System.out.printf("Cluster up in %f seconds\n", delta);

		Path hbaseRootDir = TEST_UTIL.getDFSCluster().getFileSystem()
				.makeQualified(new Path("/hbase"));
		System.out.println("hbase.rootdir=" + hbaseRootDir);
		conf.set(HConstants.HBASE_DIR, hbaseRootDir.toString());
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		long stopInNsec = System.nanoTime();
		TEST_UTIL.shutdownMiniCluster();
		long downInNsec = System.nanoTime();
		float delta = (float) (downInNsec - stopInNsec) / 1000000000;
		System.out.printf("Cluster down in %f seconds\n", delta);
	}

	@Before
	public void setUp() throws Exception {
		this.conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
		this.fs = TEST_UTIL.getDFSCluster().getFileSystem();
		this.hbaseRootDir = new Path(conf.get(HConstants.HBASE_DIR));
		this.dir = new Path(this.hbaseRootDir, TestWALObserver.class.getName());
		this.oldLogDir = new Path(this.hbaseRootDir,
				HConstants.HREGION_OLDLOGDIR_NAME);

		if (TEST_UTIL.getDFSCluster().getFileSystem().exists(this.hbaseRootDir)) {
			TEST_UTIL.getDFSCluster().getFileSystem()
					.delete(this.hbaseRootDir, true);
		}
		TEST_UTIL.truncateTable(INDEX_TABLE);
	}

	@After
	public void tearDown() throws Exception {
		TEST_UTIL.getDFSCluster().getFileSystem()
				.delete(this.hbaseRootDir, true);
	}

	@Test
	public void testPutWithoutIndexedColumn() throws Throwable {
		HRegionInfo hri = createHRegionInfo(Bytes.toString(USER_TABLE));
		HLog log = createHLog(hri);
		long[] stamps = makeTimestamps(1);
		byte[] row = Bytes.toBytes("r1");
		Put p = new Put(row);
		p.add(USER_FAMILY[0], USER_QUALIFIER[0], stamps[0], Bytes.toBytes("Hans"));
		p.add(USER_FAMILY[0], USER_QUALIFIER[1], stamps[0], Bytes.toBytes("Muster"));
		
		writeToWAL(log, hri, p);
		verifyTableSize(INDEX_TABLE, INDEX_FAMILY, INDEX_QUALIFIER, 0);
	}

	@Test
	public void testPutWith3Versions() throws Throwable {
		HRegionInfo hri = createHRegionInfo(Bytes.toString(USER_TABLE));
		HLog log = createHLog(hri);

		long[] stamps = makeTimestamps(3);
		byte[][] values = { Bytes.toBytes("ZH"), Bytes.toBytes("BE"), Bytes.toBytes("TG") };
		byte[] row = Bytes.toBytes("r2");
		// insert 3 versions of same column
		Put p = new Put(row);
		p.add(USER_FAMILY[0], USER_QUALIFIER[2], stamps[0], values[0]);
		p.add(USER_FAMILY[0], USER_QUALIFIER[2], stamps[1], values[1]);
		p.add(USER_FAMILY[0], USER_QUALIFIER[2], stamps[2], values[2]);

		writeToWAL(log, hri, p);
		getVersionAndVerify(indexTable, row, INDEX_FAMILY, INDEX_QUALIFIER, stamps[0], values[0]);
		getVersionAndVerify(indexTable, row, INDEX_FAMILY, INDEX_QUALIFIER, stamps[1], values[1]);
		getVersionAndVerify(indexTable, row, INDEX_FAMILY, INDEX_QUALIFIER, stamps[2], values[2]);
	}
	
	@Test
	public void testDeleteEntireRow() throws Throwable {
		HRegionInfo hri = createHRegionInfo(Bytes.toString(USER_TABLE));
		HLog log = createHLog(hri);
		
		long[] stamps = makeTimestamps(3);
		byte[][] values = { Bytes.toBytes("ZH"), Bytes.toBytes("BE"), Bytes.toBytes("TG") };
		byte[] row = Bytes.toBytes("r3");
		// insert 3 versions of same column
		Put p = new Put(row);
		p.add(USER_FAMILY[0], USER_QUALIFIER[2], stamps[0], values[0]);
		p.add(USER_FAMILY[0], USER_QUALIFIER[2], stamps[1], values[1]);
		p.add(USER_FAMILY[0], USER_QUALIFIER[2], stamps[2], values[2]);

		writeToWAL(log, hri, p);
		// check versions on index table
		getVersionAndVerify(indexTable, row, INDEX_FAMILY, INDEX_QUALIFIER, stamps[0], values[0]);
		getVersionAndVerify(indexTable, row, INDEX_FAMILY, INDEX_QUALIFIER, stamps[1], values[1]);
		getVersionAndVerify(indexTable, row, INDEX_FAMILY, INDEX_QUALIFIER, stamps[2], values[2]);
		
		// delete row on main table
		Delete del = new Delete(row);
		del.deleteFamily(USER_FAMILY[0], stamps[2]+1);
		writeToWAL(log, hri, del);
		verifyTableSize(INDEX_TABLE, INDEX_FAMILY, INDEX_QUALIFIER, 0);
	}
	
	@Test
	public void testDeleteIndexedColumnVersion() throws Throwable {
		HRegionInfo hri = createHRegionInfo(Bytes.toString(USER_TABLE));
		HLog log = createHLog(hri);
		
		long[] stamps = makeTimestamps(3);
		byte[][] values = { Bytes.toBytes("ZH"), Bytes.toBytes("BE"), Bytes.toBytes("TG") };
		byte[] row = Bytes.toBytes("r4");
		// insert 3 versions of same column
		Put p = new Put(row);
		p.add(USER_FAMILY[0], USER_QUALIFIER[2], stamps[0], values[0]);
		p.add(USER_FAMILY[0], USER_QUALIFIER[2], stamps[1], values[1]);
		p.add(USER_FAMILY[0], USER_QUALIFIER[2], stamps[2], values[2]);
		
		writeToWAL(log, hri, p);
		// check versions on index table
		getVersionAndVerify(indexTable, row, INDEX_FAMILY, INDEX_QUALIFIER, stamps[0], values[0]);
		getVersionAndVerify(indexTable, row, INDEX_FAMILY, INDEX_QUALIFIER, stamps[1], values[1]);
		getVersionAndVerify(indexTable, row, INDEX_FAMILY, INDEX_QUALIFIER, stamps[2], values[2]);
		
		// delete the specified version of the specified column in main table
		Delete del = new Delete(row);
		del.deleteColumn(USER_FAMILY[0], USER_QUALIFIER[2], stamps[1]);
		writeToWAL(log, hri, del);
		// check versions on index table
		getVersionAndVerify(indexTable, row, INDEX_FAMILY, INDEX_QUALIFIER, stamps[0], values[0]);
		getVersionAndVerifyMissing(indexTable, row, INDEX_FAMILY, INDEX_QUALIFIER, stamps[1], values[1]);
		getVersionAndVerify(indexTable, row, INDEX_FAMILY, INDEX_QUALIFIER, stamps[2], values[2]);
	}
	
	@Test
	public void testDeleteNotIndexedColumnVersion() throws Throwable {
		HRegionInfo hri = createHRegionInfo(Bytes.toString(USER_TABLE));
		HLog log = createHLog(hri);
		
		long[] stamps = makeTimestamps(3);
		byte[][] values = { Bytes.toBytes("Hans"), Bytes.toBytes("George"), Bytes.toBytes("Bill") };
		byte[] row = Bytes.toBytes("r5");
		// insert 3 versions of same column
		Put p = new Put(row);
		p.add(USER_FAMILY[0], USER_QUALIFIER[0], stamps[0], values[0]);
		p.add(USER_FAMILY[0], USER_QUALIFIER[0], stamps[1], values[1]);
		p.add(USER_FAMILY[0], USER_QUALIFIER[0], stamps[2], values[2]);
		
		writeToWAL(log, hri, p);
		// check versions on index table
		verifyTableSize(INDEX_TABLE, INDEX_FAMILY, INDEX_QUALIFIER, 0);
		
		// delete the specified version of the specified column in main table
		Delete del = new Delete(row);
		del.deleteColumn(USER_FAMILY[0], USER_QUALIFIER[0], stamps[2]);
		writeToWAL(log, hri, del);
		
		// check versions on index table
		verifyTableSize(INDEX_TABLE, INDEX_FAMILY, INDEX_QUALIFIER, 0);
	}
	
	@Test
	public void testWALReplay() throws Exception {
		// TODO
	}

	private void getVersionAndVerifyMissing(HTable ht, byte[] row,
			byte[] family, byte[] qualifier, long ts, byte[] value) throws Exception{
		byte[] compRow = Bytes.add(value, Bytes.toBytes("_"), row);
		Get get = new Get(compRow);
		get.addColumn(family, qualifier);
		get.setTimeStamp(ts);
		Result result = ht.get(get);
		assertEmptyResult(result, compRow, family, qualifier, value);
	}

	private void getVersionAndVerify(HTable ht, byte[] row, byte[] family,
			byte[] qualifier, long ts, byte[] value) throws Exception {
		byte[] compRow = Bytes.add(value, Bytes.toBytes("_"), row);
		Get get = new Get(compRow);
		get.addColumn(family, qualifier);
		get.setTimeStamp(ts);
		Result result = ht.get(get);
		ht.get(get);
		assertSingleResult(result, compRow, family, qualifier, value);
	}
	
	private void assertEmptyResult(Result result, byte[] compRow,
			byte[] family, byte[] qualifier, byte[] value) {
		   assertTrue("expected an empty result but result contains " +
			        result.size() + " keys", result.isEmpty());

	}

	private void assertSingleResult(Result result, byte[] row, byte[] family,
			byte[] qualifier, byte[] value) throws Exception {
		assertTrue("Expected row [" + Bytes.toString(row) + "] " + "Got row ["
				+ Bytes.toString(result.getRow()) + "]",
				equals(row, result.getRow()));
	}

	private boolean equals(byte[] left, byte[] right) {
		if (left == null && right == null)
			return true;
		if (left == null && right.length == 0)
			return true;
		if (right == null && left.length == 0)
			return true;
		return Bytes.equals(left, right);
	}

	private void verifyTableSize(byte[] tablename, byte[] family, byte[] qualifier, long expected) throws Throwable {
		AggregationClient ac = new AggregationClient(conf);
		Scan scan = new Scan();
		scan.addColumn(family, qualifier);
		long rowCount = ac.rowCount(tablename, null, scan);
		assertEquals(expected, rowCount);
	}

	private long[] makeTimestamps(int n) {
		long[] stamps = new long[n];
		for (int i = 0; i < n; i++) {
			stamps[i] = i + 1;
		}
		return stamps;
	}

	private void writeToWAL(HLog log, HRegionInfo hri, Mutation... muts)
			throws IOException {
		Map<byte[], List<KeyValue>> familyMap = null;
		WALEdit edit = new WALEdit();

		for (Mutation mut : muts) {
			familyMap = mut.getFamilyMap();
			addFamilyMapToWALEdit(familyMap, edit);
			familyMap.clear();
		}

		HTableDescriptor htd = createHTableDescriptor(Bytes
				.toString(USER_TABLE));

		// it's the WAL write, the cp hook should occur
		long now = EnvironmentEdgeManager.currentTimeMillis();
		log.append(hri, hri.getTableName(), edit, now, htd);
	}

	private HLog createHLog(HRegionInfo hri) throws IOException {
		Path basedir = new Path(this.hbaseRootDir, Bytes.toString(USER_TABLE));
		deleteDir(basedir);
		fs.mkdirs(new Path(basedir, hri.getEncodedName()));

		return new HLog(this.fs, this.dir, oldLogDir, this.conf);
	}

	private HRegionInfo createHRegionInfo(final String tableName) {
		HTableDescriptor htd = new HTableDescriptor(tableName);
		for (int i = 0; i < USER_FAMILY.length; i++) {
			HColumnDescriptor a = new HColumnDescriptor(USER_FAMILY[i]);
			htd.addFamily(a);
		}
		return new HRegionInfo(htd.getName(), null, null, false);
	}

	private HTableDescriptor createHTableDescriptor(final String tableName) {
		HTableDescriptor htd = new HTableDescriptor(tableName);
		for (int i = 0; i < USER_FAMILY.length; i++) {
			HColumnDescriptor a = new HColumnDescriptor(USER_FAMILY[i]);
			htd.addFamily(a);
		}
		return htd;
	}

	/*
	 * @param p Directory to cleanup
	 */
	private void deleteDir(final Path p) throws IOException {
		if (this.fs.exists(p)) {
			if (!this.fs.delete(p, true)) {
				throw new IOException("Failed remove of " + p);
			}
		}
	}

	/**
	 * Copied from HRegion.
	 * 
	 * @param familyMap
	 *            map of family->edits
	 * @param walEdit
	 *            the destination entry to append into
	 */
	private void addFamilyMapToWALEdit(Map<byte[], List<KeyValue>> familyMap,
			WALEdit walEdit) {
		for (List<KeyValue> edits : familyMap.values()) {
			for (KeyValue kv : edits) {
				walEdit.add(kv);
			}
		}
	}
}
