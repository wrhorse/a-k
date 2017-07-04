package com.ak.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Created by wujinbao on 2017/6/30.
 */
public class HDFSUtilTest {

    private HDFSUtil hdfsUtil;

    @Before
    public void before(){
        hdfsUtil = HDFSUtil.getInstance();
    }

    @After
    public void after() throws IOException {
        hdfsUtil.close();
    }

    @Test
    public void testExits() throws IOException {
        String dirPath = "hdfs://cluster1/user/hive/warehouse/dim_user";
        String dirFile = "hdfs://cluster1/user/hive/warehouse/dim_user_file/user.txt";

        assertTrue(hdfsUtil.exits(dirPath));
        assertTrue(hdfsUtil.exits(dirFile));
    }

    @Test
    public void testCreateFile() throws IOException {
        String fileName = "hdfs://cluster1/user/hive/warehouse/dim_user/a.txt";
        String fileConcent = "1\t2\t3\t4\t5\t6\t7\t8\n";

        if (hdfsUtil.exits(fileName)) {
            hdfsUtil.deleteFile(fileName, true);
        }


        hdfsUtil.createFile(fileName, fileConcent.getBytes());
    }

    @Test
    public void testRenameFile() throws IOException {
        String oldFileName = "hdfs://cluster1/user/hive/warehouse/dim_user/a.txt";
        String newFileName = "hdfs://cluster1/user/hive/warehouse/dim_user/b.txt";

        hdfsUtil.renameFile(oldFileName, newFileName);
    }

    @Test public void testReadFile() throws IOException {
        String fileName = "hdfs://cluster1/user/hive/warehouse/dim_user/b.txt";
        String fileConcent = "1\t2\t3\t4\t5\t6\t7\t8\n";


        assertEquals(fileConcent, hdfsUtil.readFile(fileName));

    }


    @Test public void testCreateDir() throws IOException {
        String dirName = "hdfs://cluster1/user/hive/warehouse/dim_user_a";
        assertTrue(hdfsUtil.createDirectory(dirName));
    }

    @Test public void testDeleteFile() throws IOException {
        String dirName = "hdfs://cluster1/user/hive/warehouse/dim_user_a";
        assertTrue(hdfsUtil.deleteFile(dirName, true));
    }






}
