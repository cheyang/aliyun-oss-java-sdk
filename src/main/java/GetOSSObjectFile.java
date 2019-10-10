package com.aliyun.osscli;

/*
 * @Author: cheyang
 * @Date:   2019-10-03 23:35:48
 * @Last Modified by:   cheyang
 * @Last Modified time: 2019-10-04 12:27:32
 */


import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.Bucket;
import com.aliyun.oss.model.CannedAccessControlList;
import com.aliyun.oss.model.CreateBucketRequest;
import com.aliyun.oss.model.ListBucketsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectAcl;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.GetObjectRequest;

/**
 * This sample demonstrates how to get started with basic requests to Aliyun OSS
 * using the OSS SDK for Java.
 */
public class GetOSSObjectFile {

    private static String endpoint = "<endpoint, http://oss-cn-hangzhou.aliyuncs.com>";
    private static String accessKeyId = "<accessKeyId>";
    private static String accessKeySecret = "<accessKeySecret>";
    private static String bucketName = "test-huabei5";
    private static String key = "test.txt";

    public static void main(String[] args) throws IOException {

        endpoint = System.getenv("ENDPOINT");
        accessKeyId = System.getenv("KEY_ID");
        accessKeySecret = System.getenv("KEY_SECRET");

//        bucketName=args[0];
//        key=args[1];

        boolean metadata = true;

        ObjectMetadata meta = null;

        try{
            metadata=Boolean.valueOf(args[3]);
            System.out.println("Download the metadata: "+ metadata);
        }catch(Throwable e){
            System.out.println("Download the metadata: false");
        }

        /*
         * Constructs a client instance with your account for accessing OSS
         */
        OSS ossClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);

        System.out.println("Getting Started with OSS SDK for Java\n");

        try {

            /*
             * Download an object from your bucket
             */
            System.out.println("Downloading an object");
            long start = System.currentTimeMillis();
            if (metadata){
                meta = ossClient.getObjectMetadata(bucketName, key);
            }
            // OSSObject object = ossClient.getObject(bucketName, key);
            GetObjectRequest request = new GetObjectRequest(bucketName, key);

            long startPos = 1, endPos = 3;
            request.setRange(startPos, endPos);

            File file = new File(key);
//            file.getParentFile().mkdirs();

            meta = ossClient.getObject(request, file);
            System.out.println("Download time in ms = "+(System.currentTimeMillis()-start));
            // System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
            // System.out.println("Size: "+ object.getObjectMetadata().getContentLength());
            System.out.println("Content-Type: "  + meta.getContentType());
            System.out.println("Size: "+ meta.getContentLength());
            System.out.println("crc:" + meta.getServerCRC());
            System.out.println("Dump to file:"+file.getPath());

        } catch (OSSException oe) {
            System.out.println("Caught an OSSException, which means your request made it to OSS, "
                    + "but was rejected with an error response for some reason.");
            System.out.println("Error Message: " + oe.getErrorCode());
            System.out.println("Error Code:       " + oe.getErrorCode());
            System.out.println("Request ID:      " + oe.getRequestId());
            System.out.println("Host ID:           " + oe.getHostId());
        } catch (ClientException ce) {
            System.out.println("Caught an ClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with OSS, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ce.getMessage());
        } finally {
            /*
             * Do not forget to shut down the client finally to release all allocated resources.
             */
            ossClient.shutdown();
        }
    }


}