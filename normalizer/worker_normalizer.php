<?php
declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use PhpAmqpLib\Message\AMQPMessage;
use AssyncLab\Helpers\RabbitMQHelper;
use AssyncLab\Helpers\ZipHelper;
use AssyncLab\Helpers\S3Helper;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

$queueName = 'normalizacao_queue';

$options = getopt('', [
    's3-endpoint:',
    's3-bucket:',
    's3-access-key:',
    's3-secret-key:',
    's3-region:',
    'mq-host:',
    'mq-port:',
    'mq-user:',
    'mq-pass:',
]);

$required = [
    's3-endpoint', 's3-bucket', 's3-access-key', 's3-secret-key', 's3-region',
    'mq-host', 'mq-port', 'mq-user', 'mq-pass',
];

foreach ($required as $opt) {
    if (empty($options[$opt])) {
        fwrite(STDERR, "Parâmetro obrigatório ausente: --{$opt}\n");
        exit(1);
    }
}

AssyncLab\Helpers\S3Helper::init(
    $options['s3-endpoint'],
    $options['s3-bucket'],
    $options['s3-access-key'],
    $options['s3-secret-key'],
    $options['s3-region']
);

AssyncLab\Helpers\RabbitMQHelper::init(
    $options['mq-host'],
    (int)$options['mq-port'],
    $options['mq-user'],
    $options['mq-pass']
);

echo "[*] Iniciando worker de normalização...\n";

$connection = RabbitMQHelper::getConnection();
$channel    = $connection->channel();

$channel->queue_declare($queueName, false, true, false, false);

$callback = function (AMQPMessage $msg) {
    echo "[x] Mensagem recebida: ", $msg->getBody(), "\n";

    $data = json_decode($msg->getBody(), true, 512, JSON_THROW_ON_ERROR);
    if (!is_array($data)) {
        echo "[!] Payload inválido, descartando.\n";
        $msg->ack();
        return;
    }

    $chaveS3      = $data['chaveS3']      ?? null;
    $bucket      = $data['bucket']      ?? S3Helper::getBucket();
    $pacoteId      = $data['pacoteId']       ?? null;
    $callbackUrl = $data['callbackUrl'] ?? null;

    if (!$chaveS3 || !$callbackUrl) {
        echo "[!] Campos obrigatórios ausentes, descartando.\n";
        $msg->ack();
        return;
    }

    // ======== MONOLOG POR LOTE ========
    $logDir = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'normalizer_logs';
    if (!is_dir($logDir) && !mkdir($logDir, 0777, true) && !is_dir($logDir)) {
        fwrite(STDERR, "Não foi possível criar diretório de logs {$logDir}\n");
        $msg->ack();
        return;
    }

    $logFileName = 'pacote_' . preg_replace('/[^a-zA-Z0-9_-]/', '_', $pacoteId) . '_' . date('Ymd_His') . '.log';
    $logPath     = $logDir . DIRECTORY_SEPARATOR . $logFileName;

    $logger = new Logger('normalizer');
    $logger->pushHandler(new StreamHandler($logPath, Logger::DEBUG));

    $logger->info('Mensagem recebida para normalização', [
        'pacoteId' => $pacoteId,
        'chaveZip'   => $chaveS3,
        'bucket'   => $bucket,
        'callbackUrl' => $callbackUrl,
    ]);

    try {
        // 1) Avisar PHP: started
        enviarCallback($callbackUrl, [
            'pacoteId' => $pacoteId,
            'event'    => 'iniciado',
        ]);
        $logger->info('Callback started enviado', ['callbackUrl' => $callbackUrl]);

        // 2) Baixar ZIP bruto do S3 para /tmp
        $s3     = S3Helper::getClient();
        $tmpDir = sys_get_temp_dir() . DIRECTORY_SEPARATOR . 'normalizer_' . uniqid('', true);

        if (!mkdir($tmpDir, 0777, true) && !is_dir($tmpDir)) {
            throw new \RuntimeException("Não foi possível criar diretório temporário: {$tmpDir}");
        }

        $logger->info('Baixando ZIP do S3', ['tmpDir' => $tmpDir]);

        $zipLocalPath = $tmpDir . DIRECTORY_SEPARATOR . 'input.zip';

        $s3->getObject([
            'Bucket' => $bucket,
            'Key'    => $chaveS3,
            'SaveAs' => $zipLocalPath,
        ]);

        $logger->info('ZIP baixado', ['zipLocalPath' => $zipLocalPath]);

        $partes = explode('/', $chaveS3);
        $nomeCliente = $partes[0];

        $chaveNormalizadas = $nomeCliente . '/saeb/normalizadas/' . $pacoteId . '/';
        $totalImagens     = ZipHelper::processarZip($zipLocalPath, $chaveNormalizadas, $pacoteId);

        $logger->info('ZIP processado', [
            'PrefixoS3' => $chaveS3,
            'totalImagens'     => $totalImagens,
        ]);

        if ($totalImagens === 0) {
            throw new \RuntimeException('Nenhuma imagem normalizada enviada para o bucket.');
        }

        // 4) Avisar PHP: finished (usa callbackUrl original)
        enviarCallback($callbackUrl, [
            'event'            => 'finalizado',
            'chaveNormalizadas' => $chaveNormalizadas,
            'totalImagens'     => $totalImagens,
            'pacoteId'   => $pacoteId,
        ]);

        $logger->info('Finalização do Processamento', [
            'inputPath' => "s3://{$bucket}/{$chaveS3}}",
            'callbackUrl' => $callbackUrl
        ]);

        // 6) Subir log para o S3
        foreach ($logger->getHandlers() as $handler) {
            if ($handler instanceof StreamHandler) {
                $handler->close();
            }
        }

        $zipDir = dirname($chaveS3);
        $logS3Key = "{$zipDir}/logs/{$logFileName}";
        S3Helper::uploadFile($logPath, $logS3Key);
        $logger->info('Log enviado para S3', ['logKey' => $logS3Key]);

        if (file_exists($logPath)) {
            @unlink($logPath);
        }
        limparDiretorioRecursivo($tmpDir);
        $msg->ack();

        echo "[✓] Lote {$pacoteId} normalizado ({$totalImagens} imagens).\n";
    } catch (\Throwable $e) {
        $logger->error('Erro ao processar lote', ['exception' => $e->getMessage()]);

        foreach ($logger->getHandlers() as $handler) {
            if ($handler instanceof StreamHandler) {
                $handler->close();
            }
        }

        $logS3Key = "{$zipDir}/logs/{$logFileName}";
        S3Helper::uploadFile($logPath, $logS3Key);

        if ($callbackUrl) {
            enviarCallback($callbackUrl, [
                'pacoteId'     => $pacoteId,
                'event'        => 'error',
                'errorMessage' => $e->getMessage(),
                'logKey'       => $logS3Key,
            ]);
        }

        if (file_exists($logPath)) {
            @unlink($logPath);
        }

        $msg->ack();
        echo "[!] Erro ao processar pacoteId #{$pacoteId}: {$e->getMessage()}\n";
    }
};

$channel->basic_qos((int)null, 1, null);
$channel->basic_consume($queueName, '', false, false, false, false, $callback);

echo "[*] Aguardando mensagens em {$queueName}. Para sair, CTRL+C.\n";

while ($channel->is_consuming()) {
    $channel->wait();
}

$channel->close();
try {
    $connection->close();
} catch (Exception $e) {

}

/**
 * Callback HTTP para o PHP web.
 */
function enviarCallback(string $url, array $payload): void
{
    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_POST           => true,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_HTTPHEADER     => ['Content-Type: application/json'],
        CURLOPT_POSTFIELDS     => json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        CURLOPT_TIMEOUT        => 10,
    ]);

    $response = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);

    if ($response === false || $httpCode >= 400) {
        $err = curl_error($ch) ?: "HTTP {$httpCode}";
        curl_close($ch);
        throw new \RuntimeException("Falha no callback para {$url}: {$err}");
    }

    curl_close($ch);
}

/**
 * Remove diretório recursivamente.
 */
function limparDiretorioRecursivo(string $dir): void
{
    if (!is_dir($dir)) {
        return;
    }

    $files = new \RecursiveIteratorIterator(
        new \RecursiveDirectoryIterator($dir, \FilesystemIterator::SKIP_DOTS),
        \RecursiveIteratorIterator::CHILD_FIRST
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
