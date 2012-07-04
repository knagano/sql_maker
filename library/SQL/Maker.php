<?php
namespace SQL;

class Maker {
    const DEFAULT_ALIAS = 'me';

    private $logic;
    private $aliases;

    public function __construct () {
        $this->_setLogic( 'AND' );
        $this->aliases = array();
    }

    private function _setLogic ( $logic ) {
        $this->logic = sprintf( ' %s ', strtoupper( $logic ) );
    }

    private static function _quote ( $str ) {
        return "`$str`";
    }

    private function _addAlias ( $column ) {
        if ( in_array( $column, $this->aliases ) ) {
            return $column;
        }

        return sprintf(
            '`%s`.%s',
            self::DEFAULT_ALIAS,
            $column
        );
    }

    private function _insert ( $data ) {
        switch ( self::getType( $data ) ) {
            case 'hash':
                return $this->_insertFromHash( $data );
            default:
                throw new \Exception( 'Unknown type' );
        }
    }

    private function _insertFromHash ( $data ) {
        $sql = sprintf(
            '( %s ) VALUES ( %s )',
            implode( ',', array_map( 'self::_quote', array_keys( $data ) ) ),
            implode( ',', array_fill( 0, count( $data ), '?' ) )
        );
        $params = array_values( $data );

        return array( $sql, $params );
    }

    public function insert ( $table, $data ) {
        $table = self::_quote( $table );
        list( $sql, $params ) = $this->_insert( $data );
        $sql = implode( ' ', array( 'INSERT INTO', $table, $sql ) );

        return array( $sql, $params );
    }

    public function bulk_insert ( $table, $rows ) {
        if ( ! is_array( $rows ) || count( $rows ) < 1 ) {
            throw new \Exception( 'Invalid argument' );
        }

        $num_column = count( $rows[0] );

        foreach ( $rows as $row ) {
            if ( 'hash' !== self::getType( $row ) ) {
                throw new \Exception( 'Unknown type' );
            }
            if ( count( $row ) !== $num_column ) {
                throw new \Exception( 'Invalid argument' );
            }
        }

        $columns = array_keys( $rows[0] );
        $sql_columns_part = sprintf( '( %s )', implode( ',', array_map( 'self::_quote', $columns ) ) );
        $sql_values_one = sprintf( '( %s )', implode( ',', array_fill( 0, $num_column, '?' ) ) );
        $sql_values_part = implode( ',', array_fill( 0, count( $rows ), $sql_values_one ) );
        $sql = implode( ' ', array( 'INSERT INTO', self::_quote( $table ), $sql_columns_part, 'VALUES', $sql_values_part ) );

        $params = array();
        foreach ( $rows as $row ) {
            foreach ( array_values( $row ) as $param ) { $params[] = $param; }
        }

        return array( $sql, $params );
    }

    public function upsert ( $table, $data ) {
        if ( self::getType( $data ) !== 'hash' ) {
            throw new \Exception( 'Unknown type' );
        }

        list( $sql, $params ) = $this->insert( $table, $data );

        $on_duplicate = array();
        foreach ( array_keys( $data ) as $column ) {
            $quoted = self::_quote( $column );
            $on_duplicate[] = sprintf( '%s = VALUES(%s)', $quoted, $quoted );
        }
        $sql .= ' ON DUPLICATE KEY UPDATE ' . implode( ', ', $on_duplicate );

        return array( $sql, $params );
    }

    public function upsertWithOp ( $table, $data, $opData ) {
        if ( self::getType( $data ) !== 'hash' ) {
            throw new \Exception( 'Unknown type' );
        }

        list( $sql, $params ) = $this->insert( $table, $data );

        $on_duplicate = array();
        foreach ( $opData as $key => $val ) {
            $quoted = self::_quote( $key );
            $on_duplicate[] = sprintf( '%s = %s %s', $quoted, $quoted, $val );
        }
        $sql .= ' ON DUPLICATE KEY UPDATE ' . implode( ', ', $on_duplicate );

        return array( $sql, $params );
    }

    private function _update ( $data ) {
        switch ( self::getType( $data ) ) {
            case 'hash':
                return $this->_updateFromHash( $data );
            default:
                throw new \Exception( 'Unknown type' );
        }
    }

    private function _updateFromHash ( $data ) {
        $set    = array();
        $params = array();
        foreach ( $data as $column => $value ) {
            $column = self::_quote( $column );
            array_push( $set, sprintf( '%s = ?', $column ) );
            array_push( $params, $value );
        }

        $sql = implode( ',', $set );

        return array( $sql, $params );
    }

    public function update ( $table, $data, $where = array(), $attrs = array() ) {
        $table = self::_quote( $table );
        list( $sql, $params ) = $this->_update( $data );
        $sql = implode( ' ', array( 'UPDATE', $table, 'SET', $sql ) );

        if ( count( $where ) ) {
            list( $whereSql, $whereParams ) = $this->where( $where, $attrs );
            $sql .= $whereSql;
            $params = array_merge( $params, $whereParams );
        }

        return array( $sql, $params );
    }

    private function _fieldsFromHash ( $fields ) {
        $func = '';
        $as   = '';
        foreach ( $fields as $key => $value ) {
            if ( preg_match( '/^-(as)/i', $key, $matches ) ) {
                $alias = self::_quote( $value );
                array_push( $this->aliases, $alias );
                $as = sprintf(
                    ' %s %s',
                    strtoupper( $matches[1] ),
                    $alias
                );
                continue;
            }

            $str = $this->_recurseFields( $value );
            if ( preg_match( '/^ AS /', $str ) ) {
                $as = $str;

                if ( preg_match( '/[\(\)\*\.]/', $key ) ) {
                    $func = $key;
                    continue;
                }

                $func = $this->_addAlias( self::_quote( $key ) );
                continue;
            }

            $func = sprintf(
                '%s( %s )',
                strtoupper( $key ),
                $str
            );
        }
        return "$func$as";
    }

    private function _recurseFields ( $fields ) {
        switch ( self::getType( $fields ) ) {
            case 'string':
                if ( preg_match( '/[\(\)\*\.]/', $fields ) ) {
                    return $fields;
                }
                return $this->_addAlias( self::_quote( $fields ) );
            case 'array':
                $f = array();
                foreach ( $fields as $field ) {
                    array_push( $f, $this->_recurseFields( $field ) );
                }
                return implode( ',', $f );
            case 'hash':
                return $this->_fieldsFromHash( $fields );
            default:
                throw new \Exception( 'Unknown type' );
        }
    }

    private function _joinFromHash ( $join ) {
        $table = '';
        $cond  = '';
        foreach ( $join as $key => $value ) {
            if ( preg_match( '/^-(inner|left)/i', $key, $matches ) ) {
                $table = sprintf(
                    ' %s JOIN %s',
                    strtoupper( $matches[1] ),
                    $this->_table( $value )
                );
            }

            if ( $key === '-on' ) {
                $cond = sprintf(
                    'ON %s = %s',
                    $this->_addAlias( self::_quote( array_shift( $value ) ) ),
                    array_shift( $value )
                );
            }
        }
        return "$table $cond";
    }

    private function _join ( $join ) {
        switch ( self::getType( $join ) ) {
            case 'array':
                $sql = '';
                foreach ( $join as $j ) {
                    $sql .= $this->_joinFromHash( $j );
                }
                return $sql;
            case 'hash':
                return $this->_joinFromHash( $join );
            default:
                throw new \Exception( 'Unknown type' );
        }
    }

    private function _table ( $table ) {
        switch ( self::getType( $table ) ) {
            case 'string':
                return sprintf(
                    '%s %s',
                    self::_quote( $table ),
                    self::_quote( self::DEFAULT_ALIAS )
                );
            case 'array':
                list( $entity, $alias ) = $table;
                return sprintf(
                    '%s %s',
                    self::_quote( $entity ),
                    self::_quote( $alias )
                );
            case 'hash':
                reset( $table );
                list( $key, $value ) = each( $table );
                $base = $this->_table( $key );
                $join = $this->_join( $value );
                return "$base$join";
            default:
                throw new \Exception( 'Unknown type' );
        }
    }

    public function select ( $table, $fields = '*', $where = array(), $attrs = array() ) {
        $table = $this->_table( $table );

        $fields = $this->_recurseFields( $fields );

        $sql    = implode( ' ', array( 'SELECT', $fields, 'FROM', $table ) );
        $params = array();

        if ( count( $where ) || count( $attrs ) ) {
            list( $whereSql, $whereParams ) = $this->where( $where, $attrs, true );
            $sql .= $whereSql;
            $params = $whereParams;
        }

        array_splice( $this->aliases, 0 );
        return array( $sql, $params );
    }

    public function delete ( $table, $where = array(), $attrs = array() ) {
        $table  = self::_quote( $table );
        $sql    = implode( ' ', array( 'DELETE', 'FROM', $table ) );
        $params = array();

        if ( count( $where ) ) {
            list( $whereSql, $whereParams ) = $this->where( $where, $attrs );
            $sql .= $whereSql;
            $params = $whereParams;
        }

        return array( $sql, $params );
    }

    public function replace ( $table, $data ) {
        $table = self::_quote( $table );
        list( $sql, $params ) = $this->_insert( $data );
        $sql = implode( ' ', array( 'REPLACE INTO', $table, $sql ) );

        return array( $sql, $params );
    }

    private function _recurseWhere ( $where, $alias ) {
        switch ( self::getType( $where ) ) {
            case 'array':
                return $this->_whereFromArray( $where, $alias );
            case 'hash':
                return $this->_whereFromHash( $where, $alias );
            default:
                throw new \Exception( 'Unknown type' );
        }
    }

    private function _whereFromArray ( $where, $alias ) {
        $clauses = array();
        $params  = array();
        foreach ( $where as $element ) {
            switch ( self::getType( $element ) ) {
                case 'string':
                    $column = $element;
                    if ( ! preg_match( '/[`\.]/', $column ) ) {
                        $column = self::_quote( $column );
                        if ( $alias ) {
                            $column = $this->_addAlias( $column );
                        }
                    }

                    $op     = '=';
                    array_push( $clauses, sprintf( '%s %s ?', $column, $op ) );
                    break;
                case 'hash':
                    list( $sql, $parameters ) = $this->_whereFromHash( $element, $alias );
                    array_push( $clauses, $sql );
                    $params = array_merge( $params, $parameters );
                    break;
                default:
                    throw new \Exception( 'Unknown type' );
            }
        }

        $sql = implode( $this->logic, $clauses );
        if ( $sql ) {
            $sql = "( $sql )";
        }

        return array( $sql, $params );
    }

    private function _whereFromHash ( $where, $alias ) {
        $clauses = array();
        $params  = array();
        foreach ( $where as $key => $value ) {
            $op     = '=';
            $holder = '?';
            switch ( self::getType( $value ) ) {
                case 'integer':
                    array_push( $params, $value );
                    break;
                case 'string':
                    array_push( $params, $value );
                    break;
                case 'array':
                    if ( preg_match( '/^-(and|or)/i', $key, $matches ) ) {
                        $logic = $this->logic;
                        $this->_setLogic( $matches[1] );
                        list( $sql, $parameters ) = $this->_whereFromArray( $value, $alias );
                        array_push( $clauses, $sql );
                        $params = array_merge( $params, $parameters );
                        $this->logic = $logic;
                    }
                    break;
                case 'hash':
                    if ( count( $value ) > 1 ) {
                        throw new \Exception( 'Unknown type' );
                    }
                    list( $op ) = array_keys( $value );
                    $p = $value[$op];
                    if ( preg_match( '/^-(in)/i', $op, $matches ) ) {
                        $params = array_merge( $params, $p );

                        $op     = strtoupper( $matches[1] );
                        $holder = implode( ',', array_fill( 0, count( $p ), '?' ) );
                        $holder = "( $holder )";
                        break;
                    }
                    if ( preg_match( '/^-(between)/i', $op, $matches ) ) {
                        $params = array_merge( $params, $p );

                        $op     = strtoupper( $matches[1] );
                        $holder = " ? AND ? ";
                        break;
                    }
                    array_push( $params, $p );
                    break;
                default:
                    throw new \Exception( 'Unknown type' );
            }

            if ( ! preg_match( '/^-/', $key ) ) {
                $column = $key;
                if ( ! preg_match( '/[`\.]/', $column ) ) {
                    $column = self::_quote( $column );
                    if ( $alias ) {
                        $column = $this->_addAlias( $column );
                    }
                }
                array_push( $clauses, sprintf( '%s %s %s', $column, $op, $holder ) );
            }
        }

        $sql = implode( $this->logic, $clauses );
        if ( $sql ) {
            $sql = "( $sql )";
        }

        return array( $sql, $params );
    }

    private function _groupByChunks ( $group ) {
        $sql = array();

        switch ( self::getType( $group ) ) {
            case 'string':
                if ( ! preg_match( '/[`\.]/', $group ) ) {
                    $group = $this->_addAlias( self::_quote( $group ) );
                }
                array_push( $sql, $group );
                break;
            case 'array':
                foreach ( $group as $g ) {
                    $sql = array_merge(
                        $sql,
                        $this->_groupByChunks( $g )
                    );
                }
                break;
            default:
                throw new \Exception( 'Unknown type' );
        }

        return $sql;
    }

    private function _orderByChunks ( $order ) {
        $sql = array();

        switch ( self::getType( $order ) ) {
            case 'string':
                if ( ! preg_match( '/[`\.]/', $order ) ) {
                    $order = $this->_addAlias( self::_quote( $order ) );
                }
                array_push( $sql, $order );
                break;
            case 'array':
                foreach ( $order as $o ) {
                    $sql = array_merge(
                        $sql,
                        $this->_orderByChunks( $o )
                    );
                }
                break;
            case 'hash':
                if ( count( $order ) > 1 ) {
                    throw new \Exception( 'Unknown type' );
                }
                list( $key ) = array_keys( $order );
                if ( preg_match( '/^-(asc|desc)/i', $key, $matches ) ) {
                    $ordering = $this->_orderByChunks( $order[$key] );
                    array_push(
                        $sql,
                        sprintf(
                            '%s %s',
                            array_shift( $ordering ),
                            strtoupper( $matches[1] )
                        )
                    );
                }
                break;
            default:
                throw new \Exception( 'Unknown type' );
        }

        return $sql;
    }

    private function _groupBy ( $group ) {
        $sql = $this->_groupByChunks( $group );

        return sprintf(
            ' GROUP BY %s',
            implode( ',', $sql )
        );
    }

    private function _orderBy ( $order ) {
        $sql = $this->_orderByChunks( $order );

        return sprintf(
            ' ORDER BY %s',
            implode( ',', $sql )
        );
    }

    public function where ( $where, $attrs = array(), $alias = false ) {
        list( $sql, $params ) = $this->_recurseWhere( $where, $alias );
        if ( $sql ) {
            $sql = " WHERE $sql";
        }

        if ( isset( $attrs['groupBy'] ) ) {
            $sql .= $this->_groupBy( $attrs['groupBy'] );
        }

        if ( isset( $attrs['orderBy'] ) ) {
            $sql .= $this->_orderBy( $attrs['orderBy'] );
        }

        if ( isset( $attrs['rows'] ) ) {
            $sql .= $this->_limit( $attrs['rows'] );
        }

        if ( isset( $attrs['offset'] ) ) {
            $sql .= $this->_offset( $attrs['offset'] );
        }
        return array( $sql, $params );

    }

    private function _limit ( $rows ) {
        switch ( self::getType( $rows ) ) {
            case 'integer':
                return sprintf( ' LIMIT %d', $rows );
                break;
            case 'string':
                return sprintf( ' LIMIT %d', $rows );
                break;
            default:
                throw new \Exception( 'Unknown type' );
        }
    }

    private function _offset ( $num ) {
        switch ( self::getType( $num ) ) {
            case 'integer':
                return sprintf( ' OFFSET %d', $num );
                break;
            default:
                throw new \Exception( 'Unknown type' );
        }
    }

    private static function getType ( &$stuff ) {
        $type = gettype( $stuff );

        if ( $type !== 'array' ) {
            return $type;
        }

        if ( self::isHash( $stuff ) ) {
            return 'hash';
        }
        return 'array';
    }

    private static function isHash ( &$array ) {
        if ( ! is_array( $array ) ) {
            return false;
        }

        $i = 0;
        foreach( $array as $key => $v ) {
            if ( $key !== $i++ ) {
                return true;
            }
        }
        return false;
    }
}
