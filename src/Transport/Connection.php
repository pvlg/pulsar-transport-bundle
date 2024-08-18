<?php

declare(strict_types=1);

namespace Pvlg\Bundle\PulsarTransportBundle\Transport;

use Pulsar\Consumer;
use Pulsar\ConsumerOptions;
use Pulsar\Exception\MessageNotFound;
use Pulsar\Message;
use Pulsar\Options;
use Pulsar\Producer;
use Pulsar\ProducerOptions;
use Pulsar\SubscriptionType;

final class Connection
{
    private ?Producer $producer = null;
    private ?Consumer $consumer = null;
    private ?ConsumerOptions $consumerOptions = null;

    public function __construct(private string $dsn, private array $options) {}

    public function get(): iterable
    {
        try {
            $message = $this->consumer()->receive(false);
        } catch (MessageNotFound $e) {
            return;
        }

        yield $message;
    }

    public function ack(Message $message): void
    {
        $this->consumer()->ack($message);
    }

    public function nack(Message $message): void
    {
        $this->consumer()->nack($message);
    }

    public function send($encodedMessage): string
    {
        return $this->producer()->send($encodedMessage['body'], ['properties' => $encodedMessage['headers'] ?? []]);
    }

    public function producerClose(): void
    {
        $this->producer->close();
        $this->producer = null;
    }

    public function closeConsumer(): void
    {
        $this->consumer->close();
        $this->consumer = null;
    }

    public function producer(): Producer
    {
        if ($this->producer === null) {
            $options = new ProducerOptions();

            $options->setTopic($this->options['topic']);

            if (isset($this->options[Options::CONNECT_TIMEOUT])) {
                $options->setConnectTimeout($this->options[Options::CONNECT_TIMEOUT]);
            }

            if (isset($this->options[ProducerOptions::CompressionType])) {
                $options->setCompression($this->options[ProducerOptions::CompressionType]);
            }

            $producer = new Producer($this->dsn, $options);

            $producer->connect();

            $this->producer = $producer;
        }

        return $this->producer;
    }

    public function consumer(): Consumer
    {
        if ($this->consumer === null) {
            $options = $this->consumerOptions();

            $consumer = new Consumer($this->dsn, $options);

            $consumer->connect();

            $this->consumer = $consumer;
        }

        return $this->consumer;
    }

    public function consumerOptions(): ConsumerOptions
    {
        if ($this->consumerOptions === null) {
            $options = new ConsumerOptions();

            if (isset($this->options['consumer']['name'])) {
                $options->setConsumerName($this->options['consumer']['name']);
            }

            $options->setTopic($this->options['topic']);

            if (isset($this->options[Options::CONNECT_TIMEOUT])) {
                $options->setConnectTimeout($this->options[Options::CONNECT_TIMEOUT]);
            }

            if (isset($this->options['consumer'][ConsumerOptions::SUBSCRIPTION])) {
                $options->setSubscription($this->options['consumer'][ConsumerOptions::SUBSCRIPTION]);
            } else {
                $options->setSubscription('logic');
            }

            if (isset($this->options['consumer'][ConsumerOptions::SUBSCRIPTION_TYPE])) {
                $options->setSubscriptionType($this->options['consumer'][ConsumerOptions::SUBSCRIPTION_TYPE]);
            } else {
                $options->setSubscriptionType(SubscriptionType::Shared);
            }

            $this->consumerOptions = $options;
        }

        return $this->consumerOptions;
    }
}
