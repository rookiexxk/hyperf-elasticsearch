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
     *
     * @return bool
     */
    protected $hasMore;

    protected $perPage;

    protected $currentPage;

    protected $items;

    protected $totalItems;

    /**
     * Create a new paginator instance.
     *
     * @param mixed $items
     */
    public function __construct($items, int $perPage, int $currentPage, int $totalItems)
    {
        $this->items = $items;
        $this->perPage = $perPage;
        $this->currentPage = $currentPage;
        $this->totalItems = $totalItems;

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
            'total_items' => $this->totalItems,
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
     * Get the items being paginated.
     */
    public function items(): Collection
    {
        return $this->items;
    }

    /**
     * get current page.
     */
    public function currentPage(): int
    {
        return $this->currentPage;
    }

    /**
     * get per page size.
     */
    public function perPage(): int
    {
        return $this->perPage;
    }

    /**
     * get total items.
     */
    public function total(): int
    {
        return $this->totalItems;
    }

    /**
     * Set the items for the paginator.
     * @param mixed $items
     */
    protected function setItems($items): void
    {
        $this->items = $items instanceof Collection ? $items : Collection::make($items);

        $this->hasMore = $this->perPage * $this->currentPage < $this->totalItems;

        $this->items = $this->items->slice(0, $this->perPage);
    }
}
