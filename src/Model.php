<?php

declare(strict_types=1);

namespace Janartist\Elasticsearch;

use Hyperf\Collection\Collection;
use Hyperf\Context\ApplicationContext;
use Hyperf\Contract\Arrayable;
use Hyperf\Contract\Jsonable;
use JsonSerializable;

use function Hyperf\Support\call;

abstract class Model implements Arrayable, Jsonable, JsonSerializable
{
    use HasAttributes;

    protected string $index;

    protected Client $client;

    /**
     * @var string
     */
    protected $connection = 'default';

    public function __construct()
    {
        $this->client = ApplicationContext::getContainer()->get(Client::class);
    }

    /**
     * Handle dynamic method calls into the model.
     */
    public function __call(string $method, array $parameters)
    {
        return call([$this->newQuery(), $method], $parameters);
    }

    /**
     * Handle dynamic static method calls into the method.
     */
    public static function __callStatic(string $method, array $parameters)
    {
        return (new static())->{$method}(...$parameters);
    }

    public static function query(): Builder
    {
        return (new static())->newQuery();
    }

    public function newQuery(): Builder
    {
        return $this->newModelBuilder()->setModel($this);
    }

    public function getClient(): \Elasticsearch\Client
    {
        return $this->client->create($this->connection);
    }

    public function newCollection(array $models = []): Collection
    {
        return new Collection($models);
    }

    public function newInstance(): self
    {
        $model = new static();
        return $model;
    }

    /**
     * Create a new Model query builder.
     */
    public function newModelBuilder(): Builder
    {
        return new Builder();
    }

    public function getIndex(): string
    {
        return $this->index;
    }

    public function setIndex(string $index): void
    {
        $this->index = $index;
    }
}
