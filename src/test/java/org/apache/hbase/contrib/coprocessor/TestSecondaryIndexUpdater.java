package org.apache.hbase.contrib.coprocessor;

import static org.junit.Assert.assertNotNull;

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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
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
	public static final byte[] USER_TABLE = Bytes.toBytes("user");
	public static final byte[] USER_FAMILY_NAME = Bytes.toBytes("u");
	public static final byte[] INDEX_TABLE = Bytes.toBytes("index");

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

	}

	@After
	public void tearDown() throws Exception {
		TEST_UTIL.getDFSCluster().getFileSystem()
				.delete(this.hbaseRootDir, true);
	}

	@Test
	public void testWriteToWALAndUpdateIndex() throws Exception {
		HRegionInfo hri = createHRegionInfo(Bytes.toString(USER_TABLE));
		HTableDescriptor htd = createHTableDescriptor(Bytes
				.toString(USER_TABLE));
		Path basedir = new Path(this.hbaseRootDir, Bytes.toString(USER_TABLE));
		deleteDir(basedir);
		fs.mkdirs(new Path(basedir, hri.getEncodedName()));

		HLog log = new HLog(this.fs, this.dir, oldLogDir, this.conf);

		Put p = creatPutWith1Family(Bytes.toBytes("r1"));
		Map<byte[], List<KeyValue>> familyMap = p.getFamilyMap();
		WALEdit edit = new WALEdit();
		addFamilyMapToWALEdit(familyMap, edit);

		// it's the WAL write, the cp hook should occur
		long now = EnvironmentEdgeManager.currentTimeMillis();
		log.append(hri, hri.getTableName(), edit, now, htd);
		
		Get get = new Get(Bytes.toBytes("NY_r1"));
		assertNotNull(indexTable.get(get).getRow());
	}

	private HRegionInfo createHRegionInfo(final String tableName) {
		HTableDescriptor htd = new HTableDescriptor(tableName);
		HColumnDescriptor a = new HColumnDescriptor(USER_FAMILY_NAME);
		htd.addFamily(a);
		return new HRegionInfo(htd.getName(), null, null, false);
	}

	private HTableDescriptor createHTableDescriptor(final String tableName) {
		HTableDescriptor htd = new HTableDescriptor(tableName);
		HColumnDescriptor a = new HColumnDescriptor(Bytes.toBytes("a"));
		htd.addFamily(a);
		return htd;
	}

	private Put creatPutWith1Family(byte[] row) throws IOException {
		Put p = new Put(row);
		p.add(USER_FAMILY_NAME, Bytes.toBytes("firstname"),
				Bytes.toBytes("Hans"));
		p.add(USER_FAMILY_NAME, Bytes.toBytes("lastname"),
				Bytes.toBytes("Muster"));
		p.add(USER_FAMILY_NAME, Bytes.toBytes("state"), Bytes.toBytes("NY"));
		return p;
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
