<?php
namespace AssyncLab\Helpers;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

final class RabbitMQHelper
{
    private static ?AMQPStreamConnection $connection = null;
    private static string $host;
    private static int $port;
    private static string $user;
    private static string $password;

    public static function init(
        string $host,
        int $port,
        string $user,
        string $password
    ): void {
        self::$host     = $host;
        self::$port     = $port;
        self::$user     = $user;
        self::$password = $password;
    }

    public static function getConnection(): AMQPStreamConnection
    {
        if (!self::$connection) {
            if (!isset(self::$host, self::$port, self::$user, self::$password)) {
                throw new \RuntimeException('RabbitMQHelper não foi inicializado. Chame RabbitMQHelper::init(...) antes.');
            }

            self::$connection = new AMQPStreamConnection(
                self::$host,
                self::$port,
                self::$user,
                self::$password
            );
        }

        return self::$connection;
    }

    /**
     * Envia payload para uma fila RabbitMQ (producer).
     */
    public static function enviarParaFila(string $queueName, array $payload): void
    {
        $connection = self::getConnection();
        $channel    = $connection->channel();

        $channel->queue_declare($queueName, false, true, false, false);

        $jsonPayload = json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);

        $msg = new AMQPMessage(
            $jsonPayload,
            [
                'content_type'  => 'application/json',
                'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            ]
        );

        $channel->basic_publish($msg, '', $queueName);
        $channel->close();
        // Mantém a conexão aberta para reuso (worker long running)
    }
}
