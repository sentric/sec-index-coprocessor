/**
 * 
 */
package org.apache.hbase.contrib.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class TestSecondaryIndexUpdater {
	private static final HBaseTestingUtility TEST_UTIL;

	static {
		final Configuration conf = new Configuration();
		conf.setStrings(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
				SecondaryIndexUpdater.class.getName());
		TEST_UTIL = new HBaseTestingUtility(conf);
	}

	@BeforeClass
	public static void setupBeforeClass() throws Exception {
		long startInNsec = System.nanoTime();
		TEST_UTIL.startMiniCluster(1);
		long upInNsec = System.nanoTime();
		float delta = (float) (upInNsec - startInNsec) / 1000000000;
		System.out.printf("Cluster up in %f seconds\n", delta);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		long stopInNsec = System.nanoTime();
		TEST_UTIL.shutdownMiniCluster();
		long downInNsec = System.nanoTime();
		float delta = (float) (downInNsec - stopInNsec) / 1000000000;
		System.out.printf("Cluster down in %f seconds\n", delta);
	}

	@Test
	public void putShouldUpdateIndex() throws Exception {
	}
}
