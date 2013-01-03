/**
 * 
 */
package org.apache.hbase.contrib.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.WALCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.WALObserver;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

/**
 *
 */
public class SecondaryIndexUpdater implements WALObserver {

	@Override
	public void start(CoprocessorEnvironment arg0) throws IOException {

	}

	@Override
	public void stop(CoprocessorEnvironment arg0) throws IOException {

	}

	@Override
	public void postWALWrite(ObserverContext<WALCoprocessorEnvironment> arg0,
			HRegionInfo arg1, HLogKey arg2, WALEdit arg3) throws IOException {
		
		
	}

	@Override
	public boolean preWALWrite(ObserverContext<WALCoprocessorEnvironment> arg0,
			HRegionInfo arg1, HLogKey arg2, WALEdit arg3) throws IOException {

		return false;
	}

}
