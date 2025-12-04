<?php
namespace AssyncLab\Helpers;

use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use FilesystemIterator;

final class GhostscriptHelper
{
    // Ajuste o caminho para o executável do Ghostscript dentro do projeto ou no sistema
    // Exemplo com caminho absoluto:
    // private static string $gsPath = '"C:\\Program Files\\gs\\gs10.05.1\\bin\\gswin64c.exe"';
    // Exemplo com binário dentro do projeto:
    // private static string $gsPath = '"' . __DIR__ . '/../bin/ghostscript/gswin64c.exe"';
    private static string $gsPath = '"C:\\Program Files\\gs\\gs10.05.1\\bin\\gswin64c.exe"';

    /**
     * Converte um PDF em JPG(s) 848x1168 via Ghostscript e envia para o S3.
     * Retorna a quantidade de imagens enviadas.
     */
    public static function converterPdfParaImagens(string $pdfPath, string $pdfName, string $s3Prefix): int
    {
        $enviadas = 0;
        $tempDir  = sys_get_temp_dir() . DIRECTORY_SEPARATOR . uniqid('pdf_', true);

        if (!mkdir($tempDir, 0777, true) && !is_dir($tempDir)) {
            throw new \RuntimeException(sprintf('Diretório temporário não pôde ser criado: %s', $tempDir));
        }

        $outputPattern = $tempDir . DIRECTORY_SEPARATOR . 'page_%03d.jpg';
        $logFile       = $tempDir . DIRECTORY_SEPARATOR . 'gs_error.log';

        // -g848x1168 define a grade em pixels
        // -r150 e -dJPEGQ=90 equilibram qualidade × tamanho
        $cmd = sprintf(
            '%s -dSAFER -dBATCH -dNOPAUSE -sDEVICE=jpeg -r150 -dJPEGQ=90 -g848x1168 -sOutputFile="%s" "%s" 2> "%s"',
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
            $file->isDir()
                ? rmdir($file->getRealPath())
                : unlink($file->getRealPath());
        }

        rmdir($dir);
    }
}
