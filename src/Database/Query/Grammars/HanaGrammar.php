<?php

namespace DreamFactory\Core\Hana\Database\Query\Grammars;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar;

class HanaGrammar extends Grammar
{
    /**
     * All of the available clause operators.
     *
     * @var array
     */
    protected $operators = [
        '=',
        '<',
        '>',
        '<=',
        '>=',
        '!<',
        '!>',
        '<>',
        '!=',
        'like',
        'not like',
        'between',
        'ilike',
        '&',
        '&=',
        '|',
        '|=',
        '^',
        '^=',
    ];

    /**
     * Compile an aggregated select clause.
     *
     * @param  \Illuminate\Database\Query\Builder $query
     * @param  array                              $aggregate
     *
     * @return string
     */
    protected function compileAggregate(Builder $query, $aggregate)
    {
        $column = $this->columnize($aggregate['columns']);

        // If the query has a "distinct" constraint and we're not asking for all columns
        // we need to prepend "distinct" onto the column name so that the query takes
        // it into account when it performs the aggregating operations on the data.
        if ($query->distinct && $column !== '*') {
            $column = 'distinct ' . $column;
        }

        if ($column === '1') {
            $column = '*';
        }

        return 'select ' . $aggregate['function'] . '(' . $column . ') as aggregate';
    }
}
