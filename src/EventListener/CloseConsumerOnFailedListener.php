<?php

declare(strict_types=1);

namespace Pvlg\Bundle\PulsarTransportBundle\EventListener;

use Psr\Container\ContainerInterface;
use Pulsar\SubscriptionType;
use Pvlg\Bundle\PulsarTransportBundle\Stamp\NoNackStamp;
use Pvlg\Bundle\PulsarTransportBundle\Transport\PulsarTransport;
use Symfony\Component\DependencyInjection\Attribute\Autowire;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\Messenger\Event\WorkerMessageFailedEvent;
use Symfony\Component\Messenger\EventListener\SendFailedMessageForRetryListener;

use function in_array;

final class CloseConsumerOnFailedListener implements EventSubscriberInterface
{
    public function __construct(
        private SendFailedMessageForRetryListener $decorated,
        #[Autowire(service: 'messenger.receiver_locator')]
        private ContainerInterface $receiverLocator,
    ) {}

    public function onMessageFailed(WorkerMessageFailedEvent $event): void
    {
        $transport = $this->receiverLocator->get('messenger.transport.'.$event->getReceiverName());

        if (!$transport instanceof PulsarTransport) {
            $this->decorated->onMessageFailed($event);

            return;
        }

        if (in_array(
            (int) $transport->getConsumerOptions()->getSubscriptionType(),
            [
                SubscriptionType::Exclusive,
                SubscriptionType::Failover,
                SubscriptionType::Key_Shared,
            ],
            true,
        )) {
            $transport->closeConsumer();
            $event->addStamps(new NoNackStamp());
            sleep(1);
        }
    }

    public static function getSubscribedEvents(): array
    {
        return [
            WorkerMessageFailedEvent::class => ['onMessageFailed', 100],
        ];
    }
}
