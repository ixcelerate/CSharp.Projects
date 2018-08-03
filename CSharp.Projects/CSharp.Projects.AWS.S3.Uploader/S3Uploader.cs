using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;

namespace CSharp.Projects.AWS.S3.Uploader
{
    /// <summary>
    /// </summary>
    public class S3Uploader
    {
        /// <summary>
        ///     The bucket name
        /// </summary>
        private const string BucketName = "raj-dev-bucket";

        /// <summary>
        ///     The file path
        /// </summary>
        private const string FilePath = "C:\\Users\\lgopalakrishan\\Downloads\\Omnique_Historical_2017.csv";

        /// <summary>
        ///     The bucket region
        /// </summary>
        private static readonly RegionEndpoint BucketRegion = RegionEndpoint.USWest2;

        /// <summary>
        ///     Defines the entry point of the application.
        /// </summary>
        public static void Main()
        {
            const string keyName = "Test";
            UploadFileAsync().Wait();
            UploadFileAsync(keyName).Wait();
            using (var fileToUpload = new FileStream(FilePath, FileMode.Open, FileAccess.Read))
            {
                UploadFileAsync(fileToUpload, keyName).Wait();
            }

            TrackMultipartUploadProgress(BucketName, FilePath, keyName);

            TrackMultipartUploadProgress(BucketName, FilePath, keyName, true);
        }


        /// <summary>
        ///     Uploads the file asynchronous.
        /// </summary>
        /// <returns></returns>
        private static async Task UploadFileAsync()
        {
            try
            {
                var sw = Stopwatch.StartNew();
                var credentialProfileStoreChain = new CredentialProfileStoreChain();
                credentialProfileStoreChain.TryGetAWSCredentials("basic_profile", out var defaultCredentials);

                IAmazonS3 s3Client = new AmazonS3Client(defaultCredentials, BucketRegion);

                var fileTransferUtility = new TransferUtility(s3Client);
                // Option 1. Upload a file. The file name is used as the object key name.
                await fileTransferUtility.UploadAsync(FilePath, BucketName);
                sw.Stop();

                Console.WriteLine("Upload completed in {0} seconds", sw.Elapsed.TotalSeconds);
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Error encountered on server. Message:'{0}' when writing an object", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
            }
        }

        /// <summary>
        ///     Uploads the file asynchronous.
        /// </summary>
        /// <param name="keyName">Name of the key.</param>
        /// <returns></returns>
        private static async Task UploadFileAsync(string keyName)
        {
            try
            {
                var sw = Stopwatch.StartNew();
                var credentialProfileStoreChain = new CredentialProfileStoreChain();
                credentialProfileStoreChain.TryGetAWSCredentials("basic_profile", out var defaultCredentials);

                IAmazonS3 s3Client = new AmazonS3Client(defaultCredentials, BucketRegion);

                var fileTransferUtility = new TransferUtility(s3Client);

                // Option 2. Specify object key name explicitly.
                await fileTransferUtility.UploadAsync(FilePath, BucketName, keyName);
                sw.Stop();

                Console.WriteLine("UploadWithKeyName completed in {0}seconds", sw.Elapsed.TotalSeconds);
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Error encountered on server. Message:'{0}' when writing an object", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
            }
        }

        /// <summary>
        ///     Uploads the file asynchronous.
        /// </summary>
        /// <param name="fileToUpload">The file to upload.</param>
        /// <param name="keyname">The keyname.</param>
        /// <returns></returns>
        private static async Task UploadFileAsync(Stream fileToUpload, string keyname)
        {
            try
            {
                var sw = Stopwatch.StartNew();
                var credentialProfileStoreChain = new CredentialProfileStoreChain();
                credentialProfileStoreChain.TryGetAWSCredentials("basic_profile", out var defaultCredentials);

                IAmazonS3 s3Client = new AmazonS3Client(defaultCredentials, BucketRegion);

                var fileTransferUtility = new TransferUtility(s3Client);

                // Option 3. Upload data from a type of System.IO.Stream.
                await fileTransferUtility.UploadAsync(fileToUpload,
                    BucketName, keyname);

                sw.Stop();

                Console.WriteLine("UploadWithFileStream completed in {0}seconds", sw.Elapsed.TotalSeconds);
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Error encountered on server. Message:'{0}' when writing an object", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
            }
        }

        /// <summary>
        ///     Tracks the multipart upload progress.
        /// </summary>
        /// <param name="existingBucketName">Name of the existing bucket.</param>
        /// <param name="filePath">The file path.</param>
        /// <param name="keyName">Name of the key.</param>
        /// <returns></returns>
        public static string TrackMultipartUploadProgress(string existingBucketName, string filePath, string keyName)
        {
            var sw = Stopwatch.StartNew();
            var credentialProfileStoreChain = new CredentialProfileStoreChain();
            credentialProfileStoreChain.TryGetAWSCredentials("basic_profile", out var defaultCredentials);
            using (var s3Client = new AmazonS3Client(defaultCredentials, BucketRegion))
            {
                using (var transferUtility = new TransferUtility(s3Client))
                {
                    try
                    {
                        var uploadRequest = new TransferUtilityUploadRequest
                        {
                            BucketName = existingBucketName,
                            FilePath = filePath,
                            Key = keyName,
                            //PartSize = 8388608, // 8 MB.
                            PartSize = 16777216,
                            //PartSize = 6291456, // 6 MB.
                            CannedACL = S3CannedACL.PublicRead,
                            StorageClass = S3StorageClass.ReducedRedundancy
                        };
                        uploadRequest.UploadProgressEvent += uploadRequest_UploadPartProgressEvent;
                        transferUtility.Upload(uploadRequest);
                        sw.Stop();

                        Console.WriteLine("TrackMultipartUploadProgress completed in {0}seconds",
                            sw.Elapsed.TotalSeconds);
                        return "OK";
                    }
                    catch (AmazonS3Exception ex)
                    {
                        transferUtility.AbortMultipartUploads(existingBucketName, DateTime.Now.AddDays(-1));
                        return "ERROR:" + ex.Message;
                    }
                    catch (AmazonServiceException ex)
                    {
                        transferUtility.AbortMultipartUploads(existingBucketName, DateTime.Now.AddDays(-1));
                        return "NETWORK_ERROR: " + ex.Message;
                    }
                }
            }
        }

        /// <summary>
        ///     Tracks the multipart upload progress.
        /// </summary>
        /// <param name="existingBucketName">Name of the existing bucket.</param>
        /// <param name="filePath">The file path.</param>
        /// <param name="keyName">Name of the key.</param>
        /// <param name="enableTransferAccleration">if set to <c>true</c> [enable transfer accleration].</param>
        /// <returns></returns>
        public static string TrackMultipartUploadProgress(string existingBucketName, string filePath, string keyName,
            bool enableTransferAccleration)
        {
            var sw = Stopwatch.StartNew();
            var credentialProfileStoreChain = new CredentialProfileStoreChain();
            credentialProfileStoreChain.TryGetAWSCredentials("basic_profile", out var defaultCredentials);
            using (var s3Client = new AmazonS3Client(defaultCredentials, BucketRegion))
            {
                using (var transferUtility = new TransferUtility(s3Client))
                {
                    try
                    {
                        EnableTransferAcclerationOnBucket(BucketName);
                        var uploadRequest = new TransferUtilityUploadRequest
                        {
                            BucketName = existingBucketName,
                            FilePath = filePath,
                            Key = keyName,
                            //PartSize = 8388608, // 8 MB.
                            PartSize = 16777216,
                            //PartSize = 6291456, // 6 MB.
                            CannedACL = S3CannedACL.PublicRead,
                            StorageClass = S3StorageClass.ReducedRedundancy
                        };
                        uploadRequest.UploadProgressEvent += uploadRequest_UploadPartProgressEvent;
                        transferUtility.Upload(uploadRequest);
                        sw.Stop();

                        Console.WriteLine("TrackMultipartUploadProgress completed in {0} seconds",
                            sw.Elapsed.TotalSeconds);
                        return "OK";
                    }
                    catch (AmazonS3Exception ex)
                    {
                        transferUtility.AbortMultipartUploads(existingBucketName, DateTime.Now.AddDays(-1));
                        return "ERROR:" + ex.Message;
                    }
                    catch (AmazonServiceException ex)
                    {
                        transferUtility.AbortMultipartUploads(existingBucketName, DateTime.Now.AddDays(-1));
                        return "NETWORK_ERROR: " + ex.Message;
                    }
                }
            }
        }

        /// <summary>
        ///     Uploads the request upload part progress event.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="e">The e.</param>
        private static void uploadRequest_UploadPartProgressEvent(object sender, UploadProgressArgs e)
        {
            // Process event.
            Console.WriteLine("Transferred Bytes: {0} of Total Bytes: {1}", e.TransferredBytes, e.TotalBytes);
            Console.WriteLine("PercentDone: " + e.PercentDone);
        }

        /// <summary>
        ///     Enables the transfer accleration on bucket.
        /// </summary>
        /// <param name="srBucketName">Name of the sr bucket.</param>
        /// <returns></returns>
        public static bool EnableTransferAcclerationOnBucket(string srBucketName)
        {
            var credentialProfileStoreChain = new CredentialProfileStoreChain();
            credentialProfileStoreChain.TryGetAWSCredentials("basic_profile", out var defaultCredentials);
            using (var s3Client = new AmazonS3Client(defaultCredentials, BucketRegion))
            {
                var request = new PutBucketAccelerateConfigurationRequest
                {
                    BucketName = srBucketName,
                    AccelerateConfiguration = new AccelerateConfiguration
                    {
                        Status = BucketAccelerateStatus.Enabled
                    }
                };

                var response = s3Client.PutBucketAccelerateConfigurationAsync(request);
                return response != null && response.Result.HttpStatusCode == HttpStatusCode.OK;
            }
        }
    }
}