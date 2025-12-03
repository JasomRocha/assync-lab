<?php

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

final class RabbitMQHelper
{
    private static string $host = 'localhost';
    private static int $port = 5672;
    private static string $user = 'guest';
    private static string $password = 'guest';

    /**
     * Envia payload para uma fila RabbitMQ
     */
    public static function enviarParaFila(string $queueName, array $payload): void
    {
        $connection = new AMQPStreamConnection(
            self::$host,
            self::$port,
            self::$user,
            self::$password
        );

        $channel = $connection->channel();
        $channel->queue_declare($queueName, false, true, false, false);

        $jsonPayload = json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);

        $msg = new AMQPMessage(
            $jsonPayload,
            [
                'content_type' => 'application/json',
                'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            ]
        );

        $channel->basic_publish($msg, '', $queueName);
        $channel->close();
        $connection->close();
    }
}

