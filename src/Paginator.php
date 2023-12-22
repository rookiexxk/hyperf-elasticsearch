<?php

declare(strict_types=1);

namespace Janartist\Elasticsearch;

use Hyperf\Collection\Collection;
use Hyperf\Contract\Arrayable;
use Hyperf\Contract\Jsonable;
use JsonSerializable;

class Paginator implements Arrayable, JsonSerializable, Jsonable
{
    /**
     * Determine if there are more items in the data source.
     */
    protected bool $hasMore;

    protected int $perPage;

    protected int $currentPage;

    protected mixed $items;

    /**
     * Create a new paginator instance.
     */
    public function __construct(mixed $items, int $perPage, int $currentPage)
    {
        $this->items = $items;
        $this->perPage = $perPage;
        $this->currentPage = $currentPage;

        $this->setItems($items);
    }

    public function __toString(): string
    {
        return $this->toJson();
    }

    /**
     * Determine if there are more items in the data source.
     */
    public function hasMorePages(): bool
    {
        return $this->hasMore;
    }

    /**
     * Get the instance as an array.
     */
    public function toArray(): array
    {
        return [
            'current_page' => $this->currentPage,
            'per_page' => $this->perPage,
            'data' => $this->items->toArray(),
            'has_more' => $this->hasMorePages(),
        ];
    }

    /**
     * Convert the object into something JSON serializable.
     */
    public function jsonSerialize(): array
    {
        return $this->toArray();
    }

    /**
     * Convert the object to its JSON representation.
     */
    public function toJson(int $options = 0): string
    {
        return json_encode($this->jsonSerialize(), $options);
    }

    /**
     * Set the items for the paginator.
     */
    protected function setItems(mixed $items): void
    {
        $this->items = $items instanceof Collection ? $items : Collection::make($items);

        $this->hasMore = $this->items->count() > $this->perPage;

        $this->items = $this->items->slice(0, $this->perPage);
    }
}
