import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import java.util.Random;

import static com.aliyun.oss.internal.OSSConstants.DEFAULT_BUFFER_SIZE;

public class GetOSSObjectStream {
    private static String endpoint = "<endpoint, http://oss-cn-hangzhou.aliyuncs.com>";
    private static String accessKeyId = "<accessKeyId>";
    private static String accessKeySecret = "<accessKeySecret>";
    private static String bucketName = "test-huabei5";
    private static String key = "test.txt";

    public static void main(String[] args) throws IOException, InterruptedException {

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

            long startPos = 0, endPos = -1;
            request.setRange(startPos, endPos);

            File file = new File(key);
//            file.getParentFile().mkdirs();

            OSSObject object = ossClient.getObject(request);
            InputStream inputStream = object.getObjectContent();
            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE*1024];

            int readHalfOfData = (int)endPos/2;

            int read = inputStream.read(buffer, 0, readHalfOfData);
            System.out.println("Try to read "+ read + " in the first time");
            int randomEnd = getRandomNumberInRange(0, readHalfOfData);
            int randomStart = getRandomNumberInRange(0, randomEnd);

            long startTime = System.currentTimeMillis();

            while (true) {
                read = inputStream.read(buffer, randomStart, randomEnd);
                System.out.println("Try to read "+ read +", and sleep 3s");
                Thread.sleep(3000);
                long current = System.currentTimeMillis();
                if (current - startTime > 60000){
                    System.out.println("Jump from half read at ");
                    break;
                }
            }





            meta = object.getObjectMetadata();
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

    private static int getRandomNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min)) + min;
    }
}
