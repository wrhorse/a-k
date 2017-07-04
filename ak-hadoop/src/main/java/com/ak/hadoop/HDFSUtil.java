package com.ak.hadoop;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class HDFSUtil {

    private static Logger logger = LoggerFactory.getLogger(HDFSUtil.class);

    private static HDFSUtil instance;

    private Configuration conf;

    private FileSystem fileSystem;

    private HDFSUtil() {
        try{
            this.conf = new Configuration();
            this.fileSystem = FileSystem.newInstance(conf);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * path exits or not
     *
     * @param path
     * @return
     * @throws IOException
     */

    public boolean exits(String path) throws IOException {
        return this.fileSystem.exists(new Path(path));
    }


    /**
     * create file
     * @param filePath
     * @param bytes
     * @throws IOException
     */
    public void createFile(String filePath, byte[] bytes) throws IOException {
        FSDataOutputStream outputStream = this.fileSystem.create(new Path(filePath));
        outputStream.write(bytes);
    }


    /**
     * create file
     * @param filePath
     * @param contents
     * @throws IOException
     */
    public void createFile(String filePath, String contents) throws IOException {
        createFile(filePath, contents.getBytes());
    }


    /**
     * copy from local file
     * @param localFilePath
     * @param remoteFile
     * @throws IOException
     */
    public void copyFromLocalFile(String localFilePath, String remoteFile) throws IOException {

        this.fileSystem.copyFromLocalFile(true, true, new Path(localFilePath), new Path(remoteFile));

    }


    /**
     * delete remote file
     * @param remoteFilePath
     * @param recursive
     * @return
     * @throws IOException
     */
    public boolean deleteFile(String remoteFilePath, boolean recursive) throws IOException {

        return this.fileSystem.delete(new Path(remoteFilePath), recursive);

    }

    /**
     * rename file
     * @param oldFileName
     * @param newFileName
     * @return
     * @throws IOException
     */
    public boolean renameFile(String oldFileName, String newFileName) throws IOException {

        return this.fileSystem.rename(new Path(oldFileName), new Path(newFileName));

    }


    /**
     * mkdir
     * @param dirName
     * @return
     * @throws IOException
     */
    public boolean createDirectory(String dirName) throws IOException {


        return this.fileSystem.mkdirs(new Path(dirName));

    }

    /**
     * list file , not recusive
     * @param filePath
     * @return
     * @throws IOException
     */
    public RemoteIterator<LocatedFileStatus> listFiles(String filePath, boolean recursive) throws IOException {

        return this.fileSystem.listFiles(new Path(filePath), recursive);
    }


    /**
     * list file status
     * @param dirPath
     * @return
     * @throws IOException
     */
    public FileStatus[] listFileStatus(String dirPath) throws IOException {
        return this.fileSystem.listStatus(new Path(dirPath));
    }


    /**
     * read file
     * @param filePath
     * @return
     * @throws IOException
     */
    public String readFile(String filePath) throws IOException {
        String fileContent;
        InputStream inputStream = null;
        ByteArrayOutputStream byteArrayOutputStream = null;

        try {
            inputStream = fileSystem.open(new Path(filePath));
            byteArrayOutputStream = new ByteArrayOutputStream(inputStream.available());
            IOUtils.copyBytes(inputStream, byteArrayOutputStream, this.conf);
            fileContent = byteArrayOutputStream.toString();
        }finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(byteArrayOutputStream);
        }

        return fileContent;
    }

    /**
     * close fileSystem
     * @throws IOException
     */

    public void close() throws IOException {
        if(this.fileSystem != null){
            this.fileSystem.close();
        }
    }

    /**
     * 加锁获取实例
     * @return
     */
    public static synchronized HDFSUtil getInstance() {
        if (instance == null) {
            instance = new HDFSUtil();
        }
        return instance;
    }
}
