<?php

declare(strict_types=1);

namespace Pvlg\Bundle\PulsarTransportBundle\Stamp;

use Symfony\Component\Messenger\Stamp\StampInterface;

final class NoNackStamp implements StampInterface {}
