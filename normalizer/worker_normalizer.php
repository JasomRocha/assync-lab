<?php

declare(strict_types=1);

require __DIR__ . '/../../vendor/autoload.php';

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

$rabbitHost = 'localhost';
$rabbitPort = 5672;
$rabbitUser = 'guest';
$rabbitPass = 'guest';
$queueName  = 'normalizacao_queue';

echo "[*] Iniciando worker de normalização...\n";

$connection = new AMQPStreamConnection($rabbitHost, $rabbitPort, $rabbitUser, $rabbitPass);
$channel    = $connection->channel();

$channel->queue_declare($queueName, false, true, false, false);

$callback = function (AMQPMessage $msg) {
    echo "[x] Mensagem recebida: ", $msg->body, "\n";

    $data = json_decode($msg->body, true);
    if (!is_array($data)) {
        echo "[!] Payload inválido, descartando.\n";
        $msg->ack();
        return;
    }

    // Exemplo de campos esperados:
    // nomeLote, zipKey, bucket, callbackUrl
    $nomeLote    = $data['nomeLote']    ?? null;
    $zipKey      = $data['zipKey']      ?? null;
    $bucket      = $data['bucket']      ?? 'dadoscorretor';
    $callbackUrl = $data['callbackUrl'] ?? null;

    if (!$nomeLote || !$zipKey || !$callbackUrl) {
        echo "[!] Campos obrigatórios ausentes, descartando.\n";
        $msg->ack();
        return;
    }

    try {
        // 1) Avisar PHP: started
        enviarCallback($callbackUrl, [
            'nomeLote' => $nomeLote,
            'event'    => 'started',
        ]);

        // 2) TODO: baixar ZIP do S3, extrair, chamar Ghostscript, gerar imagens normalizadas
        // 3) TODO: subir imagens normalizadas para o bucket (prefixo normalizado)
        $normalizedPrefix = "normalizados/{$nomeLote}/"; // depois ajustamos o padrão

        // 4) Avisar PHP: finished
        enviarCallback($callbackUrl, [
            'nomeLote'        => $nomeLote,
            'event'           => 'finished',
            'normalizedPrefix'=> $normalizedPrefix,
        ]);

        // 5) TODO: enviar mensagem para o Java (respostas_queue) com inputPath...

        $msg->ack();
        echo "[✓] Lote {$nomeLote} processado (normalização concluída).\n";
    } catch (Throwable $e) {
        echo "[!] Erro ao processar lote {$nomeLote}: {$e->getMessage()}\n";

        if ($callbackUrl) {
            // Tenta avisar erro de normalização
            enviarCallback($callbackUrl, [
                'nomeLote'    => $nomeLote,
                'event'       => 'error',
                'errorMessage'=> $e->getMessage(),
            ]);
        }

        // Dependendo da estratégia, você pode:
        // - reencolar (nack + requeue = true)
        // - ou descartar (ack) para não travar a fila
        $msg->ack();
    }
};

$channel->basic_qos(null, 1, null);
$channel->basic_consume($queueName, '', false, false, false, false, $callback);

echo "[*] Aguardando mensagens em {$queueName}. Para sair, CTRL+C.\n";

while ($channel->is_consuming()) {
    $channel->wait();
}

$channel->close();
$connection->close();

/**
 * Envia callback HTTP simples (POST JSON).
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
        throw new RuntimeException("Falha no callback para {$url}: {$err}");
    }

    curl_close($ch);
}
