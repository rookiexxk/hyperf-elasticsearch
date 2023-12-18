<?php

declare(strict_types=1);

namespace Janartist\Elasticsearch;

use Elasticsearch\ClientBuilder;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\ContainerInterface;
use Hyperf\Guzzle\RingPHP\PoolHandler;
use Swoole\Coroutine;

use function Hyperf\Support\make;

class Client
{
    /**
     * @var ContainerInterface
     */
    protected $container;

    /**
     * @var array
     */
    protected $config;

    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;
        $this->config = $container->get(ConfigInterface::class)->get('elasticsearch', []);
    }

    /**
     * @param string $group
     * @return \Elasticsearch\Client
     */
    public function create($group = 'default')
    {
        $config = $this->config[$group] ?? [];
        if (empty($config)) {
            throw new InvalidConfigException('elasticsearch config empty!');
        }
        $builder = ClientBuilder::create();
        if (Coroutine::getCid() > 0) {
            $handler = make(PoolHandler::class, [
                'option' => [
                    'max_connections' => $config['max_connections'] ?? 50,
                    'timeout' => $config['timeout'] ?? 0,
                ],
            ]);
            $builder->setHandler($handler);
        }

        return $builder->setHosts($config['hosts'])
            ->build();
    }
}
