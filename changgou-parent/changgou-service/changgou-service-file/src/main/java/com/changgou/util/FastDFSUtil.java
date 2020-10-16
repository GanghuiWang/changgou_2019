package com.changgou.util;

import com.changgou.file.FastDFSFile;
import org.csource.common.NameValuePair;
import org.csource.fastdfs.*;

import org.springframework.core.io.ClassPathResource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;


/**
 * 实现FastDFS文件管理
 *      文件上传
 *      文件删除
 *      文件下载
 *      文件信息获取
 *      storage信息获取
 *      tracker信息获取
 */
public class FastDFSUtil {
    /**
     * 加载Tracker链接信息
     */
    static {
        try {
            //读取fdfs_client.conf文件所在的位置
            String fileName = new ClassPathResource("fdfs_client.conf").getPath();
            ClientGlobal.init(fileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 文件上传
     * @param fastDFSFile 上传的信息文件封装
     */
    public static  String[] upload(FastDFSFile fastDFSFile) throws Exception {
        //附加参数
        NameValuePair[] meta_list = new NameValuePair[1];
        meta_list[0] = new NameValuePair("author",fastDFSFile.getAuthor());
        TrackerServer trackerServer = trackerServer();
        //通过TrackerServer的链接信息可以获取Storage的链接信息，创建storageClient对象存储storage信息
        StorageClient storageClient = storageClient(trackerServer);
        /**
         * 通过storageClient访问Storage,实现文件上传，并且获取文件上传后的存储信息
         * 1:上传文件的字节数字组
         * 2:文件的扩展名
         * 3:附加参数  比如：拍摄地址：北京
         *
         * uploads[]
         *      uploads[0]: 文件上传所存储的Storage组的名字 group1
         *      uploads[1]: M002/02/44/itheima.jpg
         */
        String[] uploads = storageClient.upload_file(fastDFSFile.getContent(), fastDFSFile.getExt(), meta_list);
        return uploads;
    }

        /**
         * 获取文件信息
         * @param groupName : 文件名的信息
         * @param remoteFileName : 文件的存储路径名字
         * @return
         */
    public static FileInfo getFile(String groupName, String remoteFileName)throws Exception{
        TrackerServer trackerServer = trackerServer();
        //通过TrackerServer的链接信息可以获取Storage的链接信息，创建storageClient对象存储storage信息
        StorageClient storageClient = storageClient(trackerServer);

        //获取文件信息
        return storageClient.get_file_info(groupName, remoteFileName);
    }
    /**
     * 文件下载
     * @param groupName : 文件名的信息
     * @param remoteFileName : 文件的存储路径名字
     * @return
     */
    public static InputStream downloadFile(String groupName, String remoteFileName)throws Exception{
        TrackerServer trackerServer = trackerServer();
        //通过TrackerServer的链接信息可以获取Storage的链接信息，创建storageClient对象存储storage信息
        StorageClient storageClient = storageClient(trackerServer);
        byte[] bytes = storageClient.download_file(groupName, remoteFileName);

        return new ByteArrayInputStream(bytes);

    }

    /**
     * 文件删除
     * @param groupName : 文件名的信息
     * @param remoteFileName : 文件的存储路径名字
     * @return
     */
    public static void deleteFile(String groupName, String remoteFileName)throws Exception{
        TrackerServer trackerServer = trackerServer();
        //通过TrackerServer的链接信息可以获取Storage的链接信息，创建storageClient对象存储storage信息
        StorageClient storageClient = storageClient(trackerServer);
        storageClient.delete_file(groupName, remoteFileName);
    }

    /**
     * 获取storage信息
     */
    public static StorageServer getStorage()throws Exception{
        //创建一个Tracker访问的客户端对象TrackerClient
        TrackerClient trackerClient = new TrackerClient();
        //通过TrackerClient访问TrackerServer服务，获取链接信息
        TrackerServer trackerServer = trackerClient.getConnection();
        //通过TrackerServer的链接信息可以获取Storage的链接信息，创建storageClient对象存储storage信息
        return trackerClient.getStoreStorage(trackerServer);
    }

    /**
     * 获取storage信息
     * @return
     */
    public static ServerInfo[] getServerInfo(String groupName, String remoteFileName) throws Exception {

        //创建一个Tracker访问的客户端对象TrackerClient
        TrackerClient trackerClient = new TrackerClient();
        //通过TrackerClient访问TrackerServer服务，获取链接信息
        TrackerServer trackerServer = trackerClient.getConnection();
        //获取storage的Ip和端口信息
        return trackerClient.getFetchStorages(trackerServer, groupName, remoteFileName);
    }
    /**
     * 获取Tracker信息
     * @return
     */
    public static String getTrackerInfo() throws Exception {
        TrackerServer trackerServer = trackerServer();
        //获取storage的Ip和端口信息
        String tracker_http_ip = trackerServer.getInetSocketAddress().getHostString();
        int tracker_http_port = ClientGlobal.getG_tracker_http_port();
        String url = "http://" + tracker_http_ip + ":" + tracker_http_port;
        return url;
    }
    /**
     * 获取TrackerServer
     * @return
     */
    public static TrackerServer trackerServer() throws Exception {

        //创建一个Tracker访问的客户端对象TrackerClient
        TrackerClient trackerClient = new TrackerClient();
        //通过TrackerClient访问TrackerServer服务，获取链接信息
        TrackerServer trackerServer = trackerClient.getConnection();
        //返回TrackServer
        return trackerServer;
    }
    /**
     * 获取StorageClient
     * @return
     */
    public static StorageClient storageClient(TrackerServer trackerServer) throws Exception {

        StorageClient storageClient = new StorageClient(trackerServer,null);
        //返回TrackServer
        return storageClient;
    }

    public static void main(String[] args)throws Exception {
        //FileInfo fileInfo = getFile("group1", "M00/00/00/wKjThF9eNXqAWLGSAADtwov5YkI7.1.jpg");
        //System.out.println(fileInfo.getCreateTimestamp()+"\n"+fileInfo.getCrc32()+"\n"
        //        +fileInfo.getFileSize()+"\n"+fileInfo.getSourceIpAddr());

        //InputStream inputStream = downloadFile("group1",
        //        "M00/00/00/wKjThF9eNXqAWLGSAADtwov5YkI7.1.jpg");
        ////将文件写入本地磁盘
        //FileOutputStream fileOutputStream = new FileOutputStream("D:/changgou/1.jpg");
        ////定义一个缓冲区
        //byte[] buffer = new byte[1024];
        //while (inputStream.read(buffer)!=-1) {
        //    fileOutputStream.write(buffer);
        //}
        ////关闭流
        //fileOutputStream.flush();
        //fileOutputStream.close();
        //inputStream.close();
        //deleteFile("group1",
        //        "M00/00/00/wKjThF9eNXqAWLGSAADtwov5YkI7.1.jpg");
        //StorageServer storageServer = getStorage();
        //System.out.println(storageServer.getStorePathIndex()+"\n"+storageServer.getInetSocketAddress());
        //ServerInfo[] groups = getServerInfo("group1",
        //        "M00/00/00/wKjThF9eNNKAXCO-AADtwov5YkI5.1.jpg");
        //for (ServerInfo group:groups
        //     ) {
        //    System.out.println(group.getIpAddr());
        //    System.out.println(group.getPort());
        //
        //}
        System.out.println("getTrackerInfo() = " + getTrackerInfo());
    }
}
