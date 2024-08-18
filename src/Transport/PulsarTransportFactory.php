<?php

declare(strict_types=1);

namespace Pvlg\Bundle\PulsarTransportBundle\Transport;

use SensitiveParameter;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

final class PulsarTransportFactory implements TransportFactoryInterface
{
    public function createTransport(#[SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        $connection = new Connection($dsn, $options);

        return new PulsarTransport($connection, $serializer);
    }

    public function supports(#[SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'pulsar://');
    }
}
