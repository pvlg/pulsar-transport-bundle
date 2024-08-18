<?php

declare(strict_types=1);

use Pvlg\Bundle\PulsarTransportBundle\EventListener\CloseConsumerOnFailedListener;
use Pvlg\Bundle\PulsarTransportBundle\Transport\PulsarTransportFactory;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

return static function (ContainerConfigurator $container): void {
    $services = $container->services();

    $services
        ->defaults()
        ->autowire()
        ->autoconfigure()
    ;

    $services
        ->set(PulsarTransportFactory::class)
        ->tag('messenger.transport_factory')
    ;

    $services
        ->set(CloseConsumerOnFailedListener::class)
        ->decorate('messenger.retry.send_failed_message_for_retry_listener')
        ->args([
            '$decorated' => service('.inner'),
        ])
    ;
};
