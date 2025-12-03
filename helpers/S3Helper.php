<?php
use Aws\S3\S3Client;
use Aws\Exception\AwsException;

final class S3Helper
{
    private static ?S3Client $client = null;
    private static string $bucket = 'dadoscorretor';
    private static string $endpoint = 'http://localhost:9000';
    private static string $accessKey = '<spaces-key>';
    private static string $secretKey = '<spaces-secret>';

    public static function getClient(): S3Client
    {
        if (!self::$client) {
            self::$client = new S3Client([
                'version'                 => 'latest',
                'region'                  => 'us-east-1',
                'endpoint'                => self::$endpoint,
                'use_path_style_endpoint' => true,
                'credentials'             => [
                    'key'    => self::$accessKey,
                    'secret' => self::$secretKey,
                ],
                'suppress_php_deprecation_warning' => true,
            ]);
        }
        return self::$client;
    }

    public static function uploadFile(string $filePath, string $key): bool
    {
        try {
            $client = self::getClient();
            $client->putObject([
                'Bucket'      => self::$bucket,
                'Key'         => $key,
                'SourceFile'  => $filePath,
                'ContentType' => mime_content_type($filePath) ?: 'image/jpeg',
            ]);
            return true;
        } catch (AwsException $e) {
            error_log("Erro S3 ao enviar {$key}: " . $e->getAwsErrorMessage());
            return false;
        }
    }
}
