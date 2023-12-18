<?php

declare(strict_types=1);

use function Hyperf\Support\env;

return [
    'default' => [
        'hosts' => [env('ELASTICSEARCH_HOST', 'http://127.0.0.1:9200')],
        'max_connections' => 50,
        'timeout' => 2.0
    ]
];
