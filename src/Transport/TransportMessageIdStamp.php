<?php

declare(strict_types=1);

namespace Pvlg\Bundle\PulsarTransportBundle\Transport;

use Symfony\Component\Messenger\Stamp\StampInterface;

final class TransportMessageIdStamp implements StampInterface
{
    public function __construct(private string $id) {}

    public function getId(): string
    {
        return $this->id;
    }
}
