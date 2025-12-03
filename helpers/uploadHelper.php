<?php

use helpers\GhostscriptHelper;
use helpers\S3Helper;
use helpers\ZipHelper;

final class uploadHelper
{
    public static function processarUpload(string $tmpPath, string $originalName, string $s3Prefix): int
    {
        $s3 = S3Helper::getClient();
        $bucket = 'dadoscorretor';

        $ext = strtolower(pathinfo($originalName, PATHINFO_EXTENSION));

        if (in_array($ext, ['jpg', 'jpeg', 'png', 'gif', 'webp'])) {
            return S3Helper::uploadFile($tmpPath, $s3Prefix . $originalName) ? 1 : 0;
        }

        if ($ext === 'pdf') {
            return GhostscriptHelper::converterPdfParaImagens($tmpPath, $originalName, $s3Prefix);
        }

        if ($ext === 'zip') {
            return ZipHelper::processarZip($tmpPath, $s3Prefix);
        }

        error_log("Tipo de arquivo não suportado: {$originalName}");
        return 0;
    }
}
