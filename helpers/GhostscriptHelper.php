<?php

use helpers\S3Helper;

final class GhostscriptHelper
{
    private static string $gsPath = '"C:\\Program Files\\gs\\gs10.05.1\\bin\\gswin64c.exe"';

    public static function converterPdfParaImagens(string $pdfPath, string $pdfName, string $s3Prefix): int
    {
        $enviadas = 0;
        $baseName = pathinfo($pdfName, PATHINFO_FILENAME);
        $tempDir  = sys_get_temp_dir() . DIRECTORY_SEPARATOR . uniqid('pdf_', true);

        if (!mkdir($tempDir, 0777, true) && !is_dir($tempDir)) {
            throw new RuntimeException(sprintf('Diretório temporário não pôde ser criado: %s', $tempDir));
        }

        $outputPattern = $tempDir . DIRECTORY_SEPARATOR . $baseName . '_page_%03d.jpg';
        $logFile       = $tempDir . DIRECTORY_SEPARATOR . 'gs_error.log';

        $cmd = sprintf(
            '%s -dSAFER -dBATCH -dNOPAUSE -sDEVICE=jpeg -r150 -sOutputFile="%s" "%s" 2> "%s"',
            self::$gsPath,
            $outputPattern,
            $pdfPath,
            $logFile
        );

        exec($cmd, $out, $ret);

        if ($ret !== 0) {
            error_log("Erro Ghostscript (ret={$ret}) ao converter {$pdfName}. Veja log em {$logFile}");
            self::limparDir($tempDir);
            return 0;
        }

        $arquivos = glob($tempDir . DIRECTORY_SEPARATOR . '*.jpg');
        if (!$arquivos) {
            error_log("Ghostscript não gerou imagens para {$pdfName}. Verifique {$logFile}.");
            self::limparDir($tempDir);
            return 0;
        }

        foreach ($arquivos as $jpgPath) {
            $fileName = basename($jpgPath);
            if (S3Helper::uploadFile($jpgPath, $s3Prefix . $fileName)) {
                $enviadas++;
            }
            @unlink($jpgPath);
        }

        if (file_exists($logFile)) {
            @unlink($logFile);
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
