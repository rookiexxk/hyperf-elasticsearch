<?php

declare(strict_types=1);

namespace Janartist\Elasticsearch;

use Elasticsearch\Client;
use Hyperf\Collection\Collection;
use Hyperf\Context\ApplicationContext;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Logger\LoggerFactory;
use InvalidArgumentException;
use stdClass;

use function Hyperf\Collection\collect;
use function Hyperf\Support\call;
use function Hyperf\Support\make;

class Builder
{
    #[Inject]
    protected Client $client;

    #[Inject]
    protected Model $model;

    protected array $query;

    protected array $highlight = [];

    protected array $sql;

    protected array $sort = [];

    protected int $take = 50;

    protected array $operate = ['=', '>', '<', '>=', '<=', '!=', '<>', 'in', 'not in', 'like', 'regex', 'prefix'];

    /**
     * 分页.
     */
    public function page(int $size = 50, int $page = 1): Paginator
    {
        $from = (($page - 1) * $size);
        $this->sql = [
            'from' => $from,
            'size' => $size,
            'index' => $this->model->getIndex(),
            'body' => [
                'query' => $this->query,
                'highlight' => $this->highlight,
                'sort' => $this->sort,
            ],
        ];
        if (empty($this->query)) {
            $this->sql = [
                'from' => $from,
                'size' => $size,
                'index' => $this->model->getIndex(),
                'body' => [
                    'query' => [
                        'match_all' => new stdClass(),
                    ],
                ],
            ];
        }
        $result = $this->run('search', $this->sql);
        $original = $result['hits']['hits'] ?? [];
        $collection = collect($original)->map(function ($value) {
            $attributes = $value['_source'] ?? [];
            $model = $this->model->newInstance();
            $model->setAttributes($attributes);
            $model->setOriginal($value);
            return $model;
        });
        return make(Paginator::class, ['items' => $collection, 'perPage' => $size, 'currentPage' => $page]);
    }

    public function get($size = 50): Collection
    {
        $this->sql = [
            'from' => 0,
            'size' => $size,
            'index' => $this->model->getIndex(),
            'body' => [
                'query' => $this->query,
                'highlight' => $this->highlight,
                'sort' => $this->sort,
            ],
        ];
        if (empty($this->query)) {
            $this->sql = [
                'from' => 0,
                'size' => $size,
                'index' => $this->model->getIndex(),
                'body' => [
                    'query' => [
                        'match_all' => new stdClass(),
                    ],
                ],
            ];
        }
        $result = $this->run('search', $this->sql);
        $original = $result['hits']['hits'] ?? [];

        return collect($original)->map(function ($value) {
            $attributes = $value['_source'] ?? [];
            $model = $this->model->newInstance();
            $model->setAttributes($attributes);
            $model->setOriginal($value);
            return $model;
        });
    }

    public function first(): Model
    {
        return $this->take(1)->get()->first();
    }

    public function take(int $take): Builder
    {
        $this->take = $take;
        return $this;
    }

    /**
     * 查找单条
     */
    public function find(mixed $id): Model
    {
        $this->sql = [
            'index' => $this->model->getIndex(),
            'id' => $id,
        ];
        $result = $this->run('get', $this->sql);

        $this->model->setAttributes($result['_source'] ?? []);
        $this->model->setOriginal($result);
        return $this->model;
    }

    /**
     * insert.
     */
    public function insert(array $values): Collection
    {
        $body = [];
        foreach ($values as $value) {
            $body['body'][] = [
                'index' => [
                    '_index' => $this->model->getIndex(),
                ],
            ];
            $body['body'][] = $value;
        }
        $this->sql = $body;
        $result = $this->run('bulk', $this->sql);
        return collect($result['items'])->map(function ($value, $key) use ($values) {
            $this->model->setAttributes(Arr::merge($values[$key] ?? [], ['_id' => $values['index']['_id'] ?? '']));
            $this->model->setOriginal($value);
            return $this->model;
        });
    }

    /**
     * create.
     */
    public function create(array $value): Model
    {
        $body = Arr::except($value, ['id', 'routing', 'timestamp']);
        $except = Arr::only($value, ['id', 'routing', 'timestamp']);
        $this->sql = Arr::merge($except, [
            'index' => $this->model->getIndex(),
            'body' => $body,
        ]);
        $result = $this->run('index', $this->sql);
        if (! empty($result['result']) && $result['result'] == 'created') {
            $this->model->setOriginal($result);
            $this->model->setAttributes(Arr::merge($body, ['_id' => $result['_id'] ?? '']));
        }
        return $this->model;
    }

    /**
     * update.
     *
     * @param string $id
     */
    public function update(array $value, $id): Model
    {
        $result = $this->run('update', [
            [
                'index' => $this->model->getIndex(),
                'id' => $id,
                'body' => [
                    'doc' => $value,
                ],
            ],
        ]);
        if (! empty($result['result']) && ($result['result'] == 'updated' || $result['result'] == 'noop')) {
            $this->model->setOriginal($result);
            $this->model->setAttributes(['_id' => $result['_id'] ?? '']);
        }
        return $this->model;
    }

    /**
     * delete.
     */
    public function delete(mixed $id): bool
    {
        $result = $this->run('delete', [
            [
                'index' => $this->model->getIndex(),
                'id' => $id,
            ],
        ]);
        if (! empty($result['result']) && $result['result'] == 'deleted') {
            return true;
        }
        return false;
    }

    public function updateMapping(array $mappings): mixed
    {
        $mappings = collect($mappings)->map(function ($value, $key) {
            $valued = [];
            if (is_string($value)) {
                $valued['type'] = $value;
            }
            if (is_array($value)) {
                $valued = $value;
            }
            return $valued;
        })->toArray();
        $this->sql = [
            'index' => $this->model->getIndex(),
            'body' => [
                'properties' => $mappings,
            ],
        ];

        return $this->run('indices.putMapping', $this->sql);
    }

    public function updateSetting(array $settings): mixed
    {
        $this->sql = [
            'index' => $this->model->getIndex(),
            'body' => [
                'settings' => $settings,
            ],
        ];
        return $this->run('putSettings', $this->sql);
    }

    public function createIndex(array $mappings = [], array $settings = []): mixed
    {
        $mappings = Arr::merge(
            collect($this->model->getCasts())->map(function ($value, $key) {
                $valued = [];
                if (is_string($value)) {
                    $valued['type'] = $value;
                }
                if (is_array($value)) {
                    $valued = $value;
                }
                return $valued;
            })->toArray(),
            collect($mappings)->map(function ($value, $key) {
                $valued = [];
                if (is_string($value)) {
                    $valued['type'] = $value;
                }
                if (is_array($value)) {
                    $valued = $value;
                }
                return $valued;
            })->toArray()
        );
        $this->sql = [
            'index' => $this->model->getIndex(),
            'id' => '1',
            'body' => compact('mappings') + compact('settings'),
        ];
        // Create the index
        return $this->run('create', $this->sql);
    }

    public function matchPhrase(string $field, mixed $value): Builder
    {
        $this->query['match_phrase'][$field] = $value;
        return $this;
    }

    public function match(string $field, mixed $value): Builder
    {
        $this->query['match'][$field] = $value;
        return $this;
    }

    /**
     * where.
     */
    public function where(string $field, string $operate, mixed $value = null): Builder
    {
        if (is_null($value)) {
            $value = $operate;
            $operate = '=';
        }
        if (in_array($operate, $this->operate)) {
            $this->parseQuery($field, $operate, $value);
        }
        return $this;
    }

    public function whereIn(string $field, array $value): Builder
    {
        return $this->where($field, 'in', $value);
    }

    public function whereNotIn(string $field, array $value): Builder
    {
        return $this->where($field, 'not in', $value);
    }

    public function whereLike(string $field, string $value): Builder
    {
        return $this->where($field, 'like', $value);
    }

    public function highlight($fields): Builder
    {
        $fields = (array) $fields;

        $fields = collect($fields)
            ->map(function ($item) {
                return [
                    $item => new stdClass(),
                ];
            })->toArray();
        $this->highlight = compact('fields');
        return $this;
    }

    /**
     * orderBy.
     */
    public function orderBy(string $field, bool $desc = false): Builder
    {
        $sort = $desc ? 'desc' : 'asc';
        if (! is_array($this->sort)) {
            $this->sort = [];
        }
        $this->sort[] = [$field => ['order' => $sort]];

        return $this;
    }

    /**
     * Set a model instance for the model being queried.
     *
     * @return $this
     */
    public function setModel(Model $model): Builder
    {
        $this->model = $model;
        $this->client = $model->getClient();
        $this->highlight = [];
        $this->sort = [];
        return $this;
    }

    /**
     * parseWhere.
     */
    protected function parseQuery(string $field, string $operate, mixed $value): Builder
    {
        switch ($operate) {
            case '=':
                $type = 'must';
                $result = ['match' => [$field => $value]];
                break;
            case '>':
                $type = 'must';
                $result = ['range' => [$field => ['gt' => $value]]];
                break;
            case '<':
                $type = 'must';
                $result = ['range' => [$field => ['lt' => $value]]];
                break;
            case '>=':
                $type = 'must';
                $result = ['range' => [$field => ['gte' => $value]]];
                break;
            case '<=':
                $type = 'must';
                $result = ['range' => [$field => ['lte' => $value]]];
                break;
            case '<>':
            case '!=':
                $type = 'must_not';
                $result = ['match' => [$field => $value]];
                break;
            case 'in':
                $type = 'must';
                $result = ['terms' => [$field => $value]];
                break;
            case 'not in':
                $type = 'must_not';
                $result = ['terms' => [$field => $value]];
                break;
            case 'like':
                $type = 'filter';
                $result = ['match_phrase' => [$field => $value]];
                break;
            case 'regex':
                $type = 'must';
                $result = ['regexp' => [$field => $value]];
                break;
            case 'prefix':
                $type = 'must';
                $result = ['prefix' => [$field => $value]];
                break;
            default:
                throw new InvalidArgumentException('invalid argument');
        }
        $this->query['bool'][$type][] = $result;
        return $this;
    }

    protected function run($method, ...$parameters): mixed
    {
        $client = $this->client;
        $sql = $this->sql;
        if (strpos($method, '.')) {
            $methods = explode('.', $method);
            $method = $methods[1];
            $client = $client->{$methods[0]}();
        }
        ApplicationContext::getContainer()
            ->get(LoggerFactory::class)
            ->get('elasticsearch', 'default')
            ->debug('Elasticsearch run', compact('method', 'parameters', 'sql'));
        return call([$client, $method], $parameters);
    }
}
