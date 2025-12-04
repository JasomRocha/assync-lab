<?php
namespace AssyncLab\Helpers;

use Aws\S3\S3Client;
use Aws\Exception\AwsException;

final class S3Helper
{
    private static ?S3Client $client = null;
    private static string $bucket;

    public static function init(
        string $endpoint,
        string $bucket,
        string $accessKey,
        string $secretKey,
        string $region
    ): void {
        self::$bucket = $bucket;

        self::$client = new S3Client([
            'version'                 => 'latest',
            'region'                  => $region,
            'endpoint'                => $endpoint,
            'use_path_style_endpoint' => true,
            'credentials'             => [
                'key'    => $accessKey,
                'secret' => $secretKey,
            ],
            'suppress_php_deprecation_warning' => true,
        ]);
    }

    public static function getClient(): S3Client
    {
        if (!self::$client) {
            throw new \RuntimeException('S3Helper não foi inicializado. Chame S3Helper::init(...) antes.');
        }
        return self::$client;
    }

    public static function getBucket(): string
    {
        if (!isset(self::$bucket)) {
            throw new \RuntimeException('S3Helper não foi inicializado. Chame S3Helper::init(...) antes.');
        }
        return self::$bucket;
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
