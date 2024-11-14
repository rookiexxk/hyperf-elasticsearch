<?php

declare(strict_types=1);

namespace Janartist\Elasticsearch;

use Hyperf\Collection\Collection;
use Hyperf\Context\ApplicationContext;
use Hyperf\Contract\Arrayable;
use Hyperf\Contract\Jsonable;
use JsonSerializable;

use function Hyperf\Support\call;

/**
 * @mixin Builder
 */
abstract class Model implements Arrayable, Jsonable, JsonSerializable
{
    use HasAttributes;

    /**
     * @var array|array<string>
     */
    protected static array $searchFields = [];

    /**
     * @var string 索引
     */
    protected $index;

    /**
     * @var Client
     */
    protected $client;

    protected array $mapping = [
        'properties' => [
        ],
    ];

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
     *
     * @param string $method
     * @param array $parameters
     */
    public function __call($method, $parameters)
    {
        return call([$this->newQuery(), $method], $parameters);
    }

    /**
     * Handle dynamic static method calls into the method.
     *
     * @param string $method
     * @param array $parameters
     */
    public static function __callStatic($method, $parameters)
    {
        return (new static())->{$method}(...$parameters);
    }

    /**
     * @return Builder
     */
    public static function query()
    {
        return (new static())->newQuery();
    }

    /**
     * @return Builder
     */
    public function newQuery()
    {
        return $this->newModelBuilder()->setModel($this);
    }

    /**
     * @return \Elasticsearch\Client
     */
    public function getClient()
    {
        return $this->client->create($this->connection);
    }

    /**
     * Create a new Model Collection instance.
     *
     * @return Collection
     */
    public function newCollection(array $models = [])
    {
        return new Collection($models);
    }

    /**
     * @return $this
     */
    public function newInstance()
    {
        $model = new static();
        return $model;
    }

    /**
     * Create a new Model query builder.
     *
     * @return Builder
     */
    public function newModelBuilder()
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

    public function getMapping()
    {
        return $this->mapping;
    }

    public static function whenQuery(array $params)
    {
        $searchFields = static::getSearchFields();
        return self::when($params, function (Builder $query) use ($params, $searchFields) {
            foreach ($params as $item) {
                [$field, $operator, $value] = $item;
                if (! in_array($field, $searchFields)) {
                    continue;
                }
                if ($operator == 'geo') {
                    $query->geo((float) $value['lat'], (float) $value['lng'], distance: $value['distance'] ?? 2000)
                        ->orderByGeo((float) $value['lat'], (float) $value['lng']);
                } else {
                    $query->where($field, $operator, $value);
                }
            }
        });
    }

    /**
     * Get all search fields.
     */
    protected static function getSearchFields(): array
    {
        if (! static::$searchFields) {
            static::$searchFields = static::getAllFields((new static())->getMapping()['properties'] ?? []);
        }

        return static::$searchFields;
    }

    protected static function getAllFields(array $mapping, string $prefix = ''): array
    {
        $fields = [];

        foreach ($mapping as $key => $value) {
            if ($key === 'properties' && is_array($value)) {
                $fields = array_merge($fields, static::getAllFields($value, $prefix));
            } elseif (is_array($value) && isset($value['type'])) {
                $fields[] = $prefix . $key;
            } elseif (is_array($value) && ! isset($value['type'])) {
                $fields = array_merge($fields, static::getAllFields($value, $prefix . $key . '.'));
            }
        }

        return $fields;
    }
}
