package com.ak.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by wujinbao on 2017/6/30.
 */
public class HDFSUtilTest {
    @Test
    public void testHDFS() throws IOException {
        HDFSUtil hdfsUtil = HDFSUtil.getInstance();


//        String newDir = "/test";
//
//        if(hdfsUtil.exits(newDir)) {
//            System.out.println(newDir + " file exits");
//        }else {
//            boolean ret = hdfsUtil.createDirectory(newDir);
//
//            if (ret) {
//                System.out.println(newDir + " create success");
//            }else {
//                System.out.println(newDir + " create fail");
//            }
//        }
//
//        String fileContent = "Hello world";
//        String fileName = newDir + "/file.txt";
//
//        hdfsUtil.createFile(fileName, fileContent);
//
//        String readFileContent = hdfsUtil.readFile(fileName);
//        System.out.println(readFileContent);
//
//
//        FileStatus[] dirs = hdfsUtil.listFileStatus("/");
//
//        for (FileStatus s: dirs) {
//            System.out.println(s);
//        }
//
//
//        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = hdfsUtil.listFiles("/", true);
//
//        while (fileStatusRemoteIterator.hasNext()) {
//            System.out.println(fileStatusRemoteIterator.next());
//        }
//
//        boolean isDeleted = hdfsUtil.deleteFile(newDir,true);
//        System.out.println(newDir + " had deleted");



//        String filePath = "hdfs://cluster1/user/hive/warehouse/static_cookie/a.txt";
//        String fileContent = hdfsUtil.readFile(filePath);
//        System.out.println(fileContent);

//        String localFile = "/Users/wujinbao/tmp/user.txt";
//        String remoteFile = "hdfs://cluster1/user/hive/warehouse/dim_user/user1.txt";
//        hdfsUtil.copyFromLocalFile(localFile, remoteFile);

    }
}
