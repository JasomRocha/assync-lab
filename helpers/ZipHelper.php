<?php
namespace AssyncLab\Helpers;

use AssyncLab\Helpers\S3Helper;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;
use FilesystemIterator;
use ZipArchive;
use RuntimeException;

final class ZipHelper
{
    /**
     * Processa um ZIP:
     * - Extrai tudo para diretório temporário
     * - Para imagens (jpg/jpeg/png): normaliza com ImageHelper (848x1168)
     * - Para PDFs: normaliza com GhostscriptHelper (848x1168)
     * - Envia todos os JPGs normalizados para o S3 usando S3Helper (via helpers chamados)
     * Retorna o total de imagens enviadas.
     */
    public static function processarZip(string $zipPath, string $s3Prefix, int $pacoteId): int
    {
        $enviadas = 0;
        $tempDir  = sys_get_temp_dir() . DIRECTORY_SEPARATOR . uniqid('zip_', true);

        if (!mkdir($tempDir, 0777, true) && !is_dir($tempDir)) {
            throw new RuntimeException(sprintf('Diretório temporário não pôde ser criado: %s', $tempDir));
        }

        $zip = new ZipArchive();
        if ($zip->open($zipPath) === true) {
            $zip->extractTo($tempDir);
            $zip->close();

            $contador = 1;

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

                if (in_array($ext, ['jpg', 'jpeg'], true)) {
                    $novoNome = "imagem-{$contador}.jpg";
                    $enviadas += ImageMagickHelper::normalizarImagem($path, $novoNome, $s3Prefix);

                    if ($enviadas > 0) {
                        $contador++;
                    }

                } elseif ($ext === 'pdf') {
                    $enviadas += GhostscriptHelper::converterPdfParaImagens($path, $pacoteId, $s3Prefix, $contador);
                } else {
                    error_log("Tipo de arquivo ignorado no ZIP: {$name}");
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
            $file->isDir()
                ? rmdir($file->getRealPath())
                : unlink($file->getRealPath());
        }

        @rmdir($dir);
    }
}
