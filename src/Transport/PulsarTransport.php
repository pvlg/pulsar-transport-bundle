<?php

declare(strict_types=1);

namespace Pvlg\Bundle\PulsarTransportBundle\Transport;

use LogicException;
use Pulsar\ConsumerOptions;
use Pvlg\Bundle\PulsarTransportBundle\Stamp\NoNackStamp;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

final class PulsarTransport implements TransportInterface
{
    public function __construct(
        private Connection $connection,
        private SerializerInterface $serializer,
    ) {}

    public function get(): iterable
    {
        $messages = $this->connection->get();
        foreach ($messages as $message) {
            $re = $message->getRedeliveryCount();
            var_dump($re);

            $envelope = $this->serializer->decode([
                'body' => $message->getPayload(),
                'headers' => $message->getProperties(),
            ]);

            yield $envelope->with(new PulsarMessageStamp($message));
        }
    }

    public function ack(Envelope $envelope): void
    {
        $stamp = $envelope->last(PulsarMessageStamp::class);
        if (!$stamp instanceof PulsarMessageStamp) {
            throw new LogicException('No PulsarMessageStamp found on the Envelope.');
        }

        $this->connection->ack($stamp->getMessage());
    }

    public function reject(Envelope $envelope): void
    {
        $pulsarMessageStamp = $envelope->last(PulsarMessageStamp::class);
        if (!$pulsarMessageStamp instanceof PulsarMessageStamp) {
            throw new LogicException('No PulsarMessageStamp found on the Envelope.');
        }

        $noNackStamp = $envelope->last(NoNackStamp::class);
        if (!$noNackStamp instanceof NoNackStamp) {
            $this->connection->consumer()->nack($pulsarMessageStamp->getMessage());
        }
    }

    public function send(Envelope $envelope): Envelope
    {
        $encodedMessage = $this->serializer->encode($envelope);

        $id = $this->connection->send($encodedMessage);

        $this->connection->producerClose();

        return $envelope->with(new TransportMessageIdStamp($id));
    }

    public function closeConsumer(): void
    {
        $this->connection->closeConsumer();
    }

    public function getConsumerOptions(): ConsumerOptions
    {
        return $this->connection->consumerOptions();
    }
}
