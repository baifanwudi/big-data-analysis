package com.demo.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.URI;

/**
 * @author allen
 * Created by allen on 18/07/2017.
 */
public abstract class AbstractFileSystem {

	private static Logger logger = LoggerFactory.getLogger(AbstractFileSystem.class);

	protected static FileSystem fileSystem = null;

	private static final String HDFS_PREFIX = "hdfs://adups";

	static {

		try {
			fileSystem = FileSystem.get(URI.create(HDFS_PREFIX), new Configuration());
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

}
