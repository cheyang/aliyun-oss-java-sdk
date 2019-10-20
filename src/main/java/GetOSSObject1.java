import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;

import com.aliyun.oss.*;
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

import static com.aliyun.oss.internal.OSSConstants.DEFAULT_BUFFER_SIZE;

/**
 * This sample demonstrates how to get started with basic requests to Aliyun OSS
 * using the OSS SDK for Java.
 */
public class GetOSSObject1 {

    private static String endpoint = "<endpoint, http://oss-cn-hangzhou.aliyuncs.com>";
    private static String accessKeyId = "<accessKeyId>";
    private static String accessKeySecret = "<accessKeySecret>";
    private static String bucketName = "train-00001-of-01024";
    private static String key = "test.txt";

    public static void main(String[] args) throws IOException, InterruptedException {

        endpoint = System.getenv("ENDPOINT");
        accessKeyId = System.getenv("KEY_ID");
        accessKeySecret = System.getenv("KEY_SECRET");

//        bucketName=args[0];
//        key=args[1];

        boolean metadata = true;
        ClientBuilderConfiguration config = new ClientBuilderConfiguration();
        config.setSocketTimeout(30000);

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
        OSS ossClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret, config);

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

            long startPos = 0, endPos = 33554431;
            int toRead = 1048576;
            request.setRange(startPos, endPos);

            File file = new File(key);
//            file.getParentFile().mkdirs();

            OSSObject object = ossClient.getObject(request);
            InputStream input = object.getObjectContent();
//            meta = object.getObjectMetadata();
            System.out.println("Download time in ms = "+(System.currentTimeMillis()-start));
            // System.out.println("Content-Type: "  + object.getObjectMetadata().getContentType());
            // System.out.println("Size: "+ object.getObjectMetadata().getContentLength());

            for (int i=0;i<31;i++){
                displayTextInputStream(input, toRead);
                Thread.sleep(2000);
            }



//            displayTextInputStream(input, 5);
//            displayTextInputStream(input, 2);
//            displayTextInputStream(
//            System.out.println("Content-Type: "  + meta.getContentType());
//            System.out.println("Size: "+ meta.getContentLength());
//            System.out.println("crc:" + meta.getServerCRC());
//            System.out.println("Dump to file:"+file.getPath());

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

    private static void displayTextInputStream(InputStream input, int length) throws IOException {

        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE*1024];

        int read = input.read(buffer, 0, length);

//        System.out.println(new String(buffer, 0, length));

        System.out.println("read len:" + read);
    }

}