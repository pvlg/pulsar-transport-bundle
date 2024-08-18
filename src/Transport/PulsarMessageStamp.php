<?php

declare(strict_types=1);

namespace Pvlg\Bundle\PulsarTransportBundle\Transport;

use Pulsar\Message;
use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

final class PulsarMessageStamp implements NonSendableStampInterface
{
    public function __construct(private Message $message) {}

    public function getMessage(): Message
    {
        return $this->message;
    }
}
