<?php

use helpers\GhostscriptHelper;
use helpers\S3Helper;

final class ZipHelper
{
    public static function processarZip(string $zipPath, string $s3Prefix): int
    {
        $enviadas = 0;
        $tempDir = sys_get_temp_dir() . DIRECTORY_SEPARATOR . uniqid('zip_', true);

        if (!mkdir($tempDir, 0777, true) && !is_dir($tempDir)) {
            throw new RuntimeException(sprintf('Diretório temporário não pôde ser criado: %s', $tempDir));
        }

        $zip = new ZipArchive();
        if ($zip->open($zipPath) === true) {
            $zip->extractTo($tempDir);
            $zip->close();

            $it = new RecursiveIteratorIterator(
                new RecursiveDirectoryIterator($tempDir, FilesystemIterator::SKIP_DOTS)
            );

            foreach ($it as $file) {
                if (!$file->isFile()) {
                    continue;
                }
                $path = $file->getRealPath();
                $name = $file->getFilename();
                $ext  = strtolower(pathinfo($name, PATHINFO_EXTENSION));

                if (in_array($ext, ['jpg', 'jpeg', 'png', 'gif', 'webp'])) {
                    if (S3Helper::uploadFile($path, $s3Prefix . $name)) {
                        $enviadas++;
                    }
                } elseif ($ext === 'pdf') {
                    $enviadas += GhostscriptHelper::converterPdfParaImagens($path, $name, $s3Prefix);
                }
            }
        } else {
            error_log("Não foi possível abrir o ZIP: {$zipPath}");
        }

        self::limparDir($tempDir);

        return $enviadas;
    }

    private static function limparDir(string $dir): void
    {
        if (!is_dir($dir)) {
            return;
        }
        $files = new RecursiveIteratorIterator(
            new RecursiveDirectoryIterator($dir, FilesystemIterator::SKIP_DOTS),
            RecursiveIteratorIterator::CHILD_FIRST
        );
        foreach ($files as $file) {
            $file->isDir() ? rmdir($file->getRealPath()) : unlink($file->getRealPath());
        }
        rmdir($dir);
    }
}
