<?php
namespace AssyncLab\Helpers;

use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use FilesystemIterator;

final class ImageMagickHelper
{
    // Caminho para o binário do ImageMagick
    // Ex.: instalado no sistema:
    // private static string $magickPath = '"C:\\Program Files\\ImageMagick-7.1.1-Q16-HDRI\\magick.exe"';
    // ou dentro do projeto:
    // private static string $magickPath = '"' . __DIR__ . '/../bin/imagemagick/magick.exe"';
    private static string $magickPath = '"C:\\Program Files\\ImageMagick-7.1.2-Q16-HDRI\\magick.exe"';

    /**
     * Normaliza uma imagem (jpg/jpeg/png) para JPG 848x1168 e envia para o S3.
     * Retorna a quantidade de imagens enviadas (0 ou 1).
     */
    public static function normalizarImagem(string $inputPath, string $inputName, string $s3Prefix): int
    {
        $tempDir = sys_get_temp_dir() . DIRECTORY_SEPARATOR . uniqid('img_', true);

        if (!mkdir($tempDir, 0777, true) && !is_dir($tempDir)) {
            throw new \RuntimeException(sprintf('Diretório temporário não pôde ser criado: %s', $tempDir));
        }

        $baseName   = pathinfo($inputName, PATHINFO_FILENAME);
        $outputPath = $tempDir . DIRECTORY_SEPARATOR . $baseName . '_norm.jpg';
        $logFile    = $tempDir . DIRECTORY_SEPARATOR . 'magick_error.log';

        // 848x1168, mantendo proporção e centralizando (crop/padding)
        $cmd = sprintf(
            '%s %s -resize 848x1168^ -gravity center -extent 848x1168 -quality 90 %s 2> %s',
            self::$magickPath,
            escapeshellarg($inputPath),
            escapeshellarg($outputPath),
            escapeshellarg($logFile)
        );

        exec($cmd, $out, $ret);

        if ($ret !== 0 || !file_exists($outputPath)) {
            error_log("Erro ImageMagick ao normalizar {$inputName}. Veja log em {$logFile}");
            self::limparDir($tempDir);
            return 0;
        }

        $enviadas = 0;
        if (S3Helper::uploadFile($outputPath, $s3Prefix . basename($outputPath))) {
            $enviadas++;
        }

        usleep(100000);
        if (file_exists($outputPath)) {
            if (!@unlink($outputPath)) {
                error_log("Aviso: não foi possível remover arquivo temporário {$outputPath}");
            }
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
            if ($file->isDir()) {
                @rmdir($file->getRealPath());
            } else {
                @unlink($file->getRealPath());
            }
        }
        @rmdir($dir);

    }
}
