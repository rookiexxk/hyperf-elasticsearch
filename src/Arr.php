<?php

declare(strict_types=1);

namespace Janartist\Elasticsearch;

class Arr extends \Hyperf\Collection\Arr
{
    /**
     * Merges two or more arrays into one recursively.
     * If each array has an element with the same string key value, the latter
     * will overwrite the former (different from array_merge_recursive).
     * Recursive merging will be conducted if both arrays have an element of array
     * type and are having the same key.
     * For integer-keyed elements, the elements from the latter array will
     * be appended to the former array.
     *
     * @param array $array1 array to be merged to
     * @param array $array2 array to be merged from. You can specify additional
     *                      arrays via third argument, fourth argument etc.
     *
     * @return array the merged array (the original arrays are not changed.)
     */
    public static function merge(array $array1, array $array2, bool $unique = true): array
    {
        $args = func_get_args();
        $res = array_shift($args);
        while (! empty($args)) {
            $next = array_shift($args);
            foreach ($next as $k => $v) {
                if (is_int($k)) {
                    if (isset($res[$k])) {
                        $res[] = $v;
                    } else {
                        $res[$k] = $v;
                    }
                } elseif (is_array($v) && isset($res[$k]) && is_array($res[$k])) {
                    $res[$k] = self::merge($res[$k], $v);
                } else {
                    $res[$k] = $v;
                }
            }
        }

        return $res;
    }
}
