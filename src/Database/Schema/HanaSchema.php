<?php

namespace DreamFactory\Core\Hana\Database\Schema;

use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Schema\FunctionSchema;
use DreamFactory\Core\Database\Schema\ParameterSchema;
use DreamFactory\Core\Database\Schema\ProcedureSchema;
use DreamFactory\Core\Database\Schema\RoutineSchema;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\DbSimpleTypes;
use DreamFactory\Core\SqlDb\Database\Schema\SqlSchema;
use Log;
/**
 * Schema is the class for retrieving metadata information from a MS SQL Server database.
 */
class HanaSchema extends SqlSchema
{
    /**
     * @const string Quoting characters
     */
    const LEFT_QUOTE_CHARACTER = '"';

    const RIGHT_QUOTE_CHARACTER = '"';

    /**
     * @inheritdoc
     */
    public function getDefaultSchema()
    {
        Log::debug('HANASchema::getDefaultSchema()');
        return $this->getUserName();
    }

    /**
     * @inheritdoc
     */
    protected function translateSimpleColumnTypes(array &$info)
    {
        /*
         * <data_type> ::=
         DATE
         | TIME
         | SECONDDATE
         | TIMESTAMP
         | TINYINT
         | SMALLINT
         | INTEGER
         | BIGINT
         | SMALLDECIMAL
         | REAL
         | DOUBLE
         | TEXT
         | BINTEXT
         | VARCHAR [ (<unsigned_integer>) ]
         | NVARCHAR [ (<unsigned_integer>) ]
         | ALPHANUM [ (<unsigned_integer>) ]
         | VARBINARY [ (<unsigned_integer>) ]
         | SHORTTEXT [ (<unsigned_integer>) ]
         | DECIMAL [ (<unsigned_integer> [, <unsigned_integer> ]) ]
         | FLOAT [ (<unsigned_integer>) ]
         | BOOLEAN

        BLOB | CLOB | NCLOB
         */
        $type = (isset($info['type'])) ? $info['type'] : null;
        switch ($type) {
            // some types need massaging, some need other required properties
            case 'pk':
            case DbSimpleTypes::TYPE_ID:
                $info['type'] = 'integer';
                $info['allow_null'] = false;
                $info['auto_increment'] = true;
                $info['is_primary_key'] = true;
                break;

            case 'fk':
            case DbSimpleTypes::TYPE_REF:
                $info['type'] = 'integer';
                $info['is_foreign_key'] = true;
                // check foreign tables
                break;

            case DbSimpleTypes::TYPE_TIMESTAMP_ON_CREATE:
                $info['type'] = 'timestamp';
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (!isset($default)) {
                    $info['default'] = ['expression' => 'CURRENT_TIMESTAMP'];
                }
                break;
            case DbSimpleTypes::TYPE_TIMESTAMP_ON_UPDATE:
                $info['type'] = 'timestamp';
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (!isset($default)) {
                    $info['default'] = ['expression' => 'CURRENT_TIMESTAMP'];
                }
                break;
            case DbSimpleTypes::TYPE_USER_ID:
            case DbSimpleTypes::TYPE_USER_ID_ON_CREATE:
            case DbSimpleTypes::TYPE_USER_ID_ON_UPDATE:
                $info['type'] = 'integer';
                break;

            case DbSimpleTypes::TYPE_BOOLEAN:
                $info['type'] = 'boolean';
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (isset($default)) {
                    $info['default'] = filter_var($default, FILTER_VALIDATE_BOOLEAN) ? 'TRUE' : 'FALSE';
                }
                break;

            case DbSimpleTypes::TYPE_INTEGER:
                $info['type'] = 'integer';
                break;

            case DbSimpleTypes::TYPE_DOUBLE:
                $info['type'] = 'float';
                $info['type_extras'] = '(53)';
                break;

            case DbSimpleTypes::TYPE_MONEY:
                $info['type'] = 'decimal';
                break;

            case DbSimpleTypes::TYPE_TEXT:
                $info['type'] = 'text';
                break;

            case DbSimpleTypes::TYPE_STRING:
                $fixed =
                    (isset($info['fixed_length'])) ? filter_var($info['fixed_length'], FILTER_VALIDATE_BOOLEAN) : false;
                $national =
                    (isset($info['supports_multibyte'])) ? filter_var($info['supports_multibyte'],
                        FILTER_VALIDATE_BOOLEAN) : false;
                if ($national) {
                    $info['type'] = 'nvarchar';
                } else {
                    $info['type'] = 'varchar';
                }
                break;

            case DbSimpleTypes::TYPE_BINARY:
                $fixed =
                    (isset($info['fixed_length'])) ? filter_var($info['fixed_length'], FILTER_VALIDATE_BOOLEAN) : false;
                $info['type'] = ($fixed) ? 'blob' : 'varbinary';
                break;
        }
    }

    protected function validateColumnSettings(array &$info)
    {
        // override this in each schema class
        $type = (isset($info['type'])) ? $info['type'] : null;
        switch ($type) {
            // some types need massaging, some need other required properties
            case 'tinyint':
            case 'smallint':
            case 'int':
            case 'integer':
            case 'bigint':
                $default = (isset($info['default'])) ? $info['default'] : null;
                if (isset($default) && is_numeric($default)) {
                    $info['default'] = intval($default);
                }
                break;

            case 'decimal':
                if (!isset($info['type_extras'])) {
                    $length =
                        (isset($info['length']))
                            ? $info['length']
                            : ((isset($info['precision'])) ? $info['precision']
                            : null);
                    if (!empty($length)) {
                        $scale =
                            (isset($info['decimals']))
                                ? $info['decimals']
                                : ((isset($info['scale'])) ? $info['scale']
                                : null);
                        $info['type_extras'] = (!empty($scale)) ? "($length,$scale)" : "($length)";
                    }
                }

                $default = (isset($info['default'])) ? $info['default'] : null;
                if (isset($default) && is_numeric($default)) {
                    $info['default'] = floatval($default);
                }
                break;
            case 'float':
                if (!isset($info['type_extras'])) {
                    $length =
                        (isset($info['length']))
                            ? $info['length']
                            : ((isset($info['precision'])) ? $info['precision']
                            : null);
                    if (!empty($length)) {
                        $info['type_extras'] = "($length)";
                    }
                }

                $default = (isset($info['default'])) ? $info['default'] : null;
                if (isset($default) && is_numeric($default)) {
                    $info['default'] = floatval($default);
                }
                break;

            case 'char':
            case 'nchar':
            case 'binary':
                $length = (isset($info['length'])) ? $info['length'] : ((isset($info['size'])) ? $info['size'] : null);
                if (isset($length)) {
                    $info['type_extras'] = "($length)";
                }
                break;

            case 'varchar':
            case 'nvarchar':
            case 'varbinary':
                $length = (isset($info['length'])) ? $info['length'] : ((isset($info['size'])) ? $info['size'] : null);
                if (isset($length)) {
                    $info['type_extras'] = "($length)";
                } else // requires a max length
                {
                    $info['type_extras'] = '(' . static::DEFAULT_STRING_MAX_SIZE . ')';
                }
                break;

            case 'time':
            case 'datetime':
            case 'timestamp':
                $length = (isset($info['length'])) ? $info['length'] : ((isset($info['size'])) ? $info['size'] : null);
                if (isset($length)) {
                    $info['type_extras'] = "($length)";
                }
                break;
        }
    }

    /**
     * @param array $info
     *
     * @return string
     * @throws \Exception
     */
    protected function buildColumnDefinition(array $info)
    {
        Log::debug("buildColumnDefinition");

        $type = (isset($info['type'])) ? $info['type'] : null;
        $typeExtras = (isset($info['type_extras'])) ? $info['type_extras'] : null;

        $definition = $type . $typeExtras;

        $allowNull = (isset($info['allow_null'])) ? filter_var($info['allow_null'], FILTER_VALIDATE_BOOLEAN) : false;
        $definition .= ($allowNull) ? ' NULL' : ' NOT NULL';

        $default = (isset($info['default'])) ? $info['default'] : null;
        if (isset($default)) {
            if (is_array($default)) {
                $expression = (isset($default['expression'])) ? $default['expression'] : null;
                if (null !== $expression) {
                    $definition .= ' DEFAULT ' . $expression;
                }
            } else {
                if (0 !== strcasecmp('boolean', $type)) {
                    $default = $this->quoteValue($default);
                }
                $definition .= ' DEFAULT ' . $default;
            }
        }

        $auto = (isset($info['auto_increment'])) ? filter_var($info['auto_increment'], FILTER_VALIDATE_BOOLEAN) : false;
        if ($auto) {
            $definition .= ' GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1)';
        }
        $isUniqueKey = (isset($info['is_unique'])) ? filter_var($info['is_unique'], FILTER_VALIDATE_BOOLEAN) : false;
        $isPrimaryKey =
            (isset($info['is_primary_key'])) ? filter_var($info['is_primary_key'], FILTER_VALIDATE_BOOLEAN) : false;
        if ($isPrimaryKey && $isUniqueKey) {
            throw new \Exception('Unique and Primary designations not allowed simultaneously.');
        }

        if ($isUniqueKey) {
            $definition .= ' UNIQUE';
        } elseif ($isPrimaryKey) {
            $definition .= ' PRIMARY KEY';
        }

        return $definition;
    }

    /**
     * Compares two table names.
     * The table names can be either quoted or unquoted. This method
     * will consider both cases.
     *
     * @param string $name1 table name 1
     * @param string $name2 table name 2
     *
     * @return boolean whether the two table names refer to the same table.
     */
    public function compareTableNames($name1, $name2)
    {
        $name1 = str_replace(['"', '"'], '', $name1);
        $name2 = str_replace(['"', '"'], '', $name2);

        return parent::compareTableNames(strtolower($name1), strtolower($name2));
    }

    /**
     * Resets the sequence value of a table's primary key.
     * The sequence will be reset such that the primary key of the next new row inserted
     * will have the specified value or max value of a primary key plus one (i.e. sequence trimming).
     *
     * @param TableSchema  $table   the table schema whose primary key sequence will be reset
     * @param integer|null $value   the value for the primary key of the next new row inserted.
     *                              If this is not set, the next new row's primary key will have the max value of a
     *                              primary key plus one (i.e. sequence trimming).
     *
     */
    public function resetSequence($table, $value = null)
    {
        if ($table->sequenceName === null) {
            return;
        }
        if ($value !== null) {
            $value = (int)($value) - 1;
        } else {
            /** @noinspection SqlNoDataSourceInspection */
            /** @noinspection SqlDialectInspection */
            $value = (int)$this->selectValue("SELECT MAX([{$table->primaryKey}]) FROM {$table->quotedName}");
        }
        $name = strtr($table->quotedName, ['[' => '', ']' => '']);
        $this->connection->statement("DBCC CHECKIDENT ('$name',RESEED,$value)");
    }

    /**
     * @inheritdoc
     */
    protected function loadTableColumns(TableSchema $table)
    {
        $schema = strtoupper(trim($table->schemaName));
        $tableName = strtoupper(trim($table->resourceName));

        $sql = <<<HANA
SELECT COLUMN_NAME, POSITION, DATA_TYPE_NAME, OFFSET, LENGTH, SCALE, IS_NULLABLE, DEFAULT_VALUE, GENERATION_TYPE
FROM PUBLIC.TABLE_COLUMNS
WHERE SCHEMA_NAME = '{$schema}' AND TABLE_NAME = '{$tableName}'
ORDER BY POSITION
HANA;

        Log::debug("Executing loadTableColumns with SQL: {$sql}");

        $columns = $this->connection->select($sql);

        foreach ($columns as $column) {
            $column = array_change_key_case((array)$column, CASE_LOWER);

            $c = new ColumnSchema(['name' => $column['column_name']]);
            $c->quotedName = $this->quoteColumnName($c->name);
            $c->allowNull = $column['is_nullable'] === 'TRUE';
            $c->autoIncrement = isset($column['generation_type']) &&
                stripos($column['generation_type'], 'IDENTITY') !== false;
            $c->dbType = $column['data_type_name'];
            $c->scale = (int) $column['scale'];
            $c->precision = $c->size = (int) $column['length'];
            $c->comment = null;

            $c->fixedLength = $this->extractFixedLength($c->dbType);
            $c->supportsMultibyte = $this->extractMultiByteSupport($c->dbType);
            $this->extractType($c, $c->dbType);

            if (isset($column['default_value'])) {
                $this->extractDefault($c, $column['default_value']);
            }

            if ($c->isPrimaryKey) {
                if ($c->autoIncrement) {
                    $table->sequenceName = array_get($column, 'sequence', $c->name);
                    if (DbSimpleTypes::TYPE_INTEGER === $c->type) {
                        $c->type = DbSimpleTypes::TYPE_ID;
                    }
                }
                $table->addPrimaryKey($c->name);
            }

            $table->addColumn($c);
        }
    }



    /**
     * @inheritdoc
     */
    protected function getTableConstraints($schema = '')
    {
        Log::debug('getTableConstraints');

        if (is_array($schema)) {
            $schema = implode("','", $schema);
        }

        $sql = <<<EOD
            SELECT constraint_name, schema_name as table_schema, table_name, column_name, is_primary_key, is_unique_key
            FROM sys.constraints 
            WHERE schema_name IN ('{$schema}')
        EOD;
        $result = $this->connection->select($sql);

        $constraints = [];
        foreach ($result as $row) {
            $row = array_change_key_case((array)$row, CASE_LOWER);
            $ts = strtolower($row['table_schema']);
            $tn = strtolower($row['table_name']);
            $cn = strtolower($row['constraint_name']);
            if ($row['is_primary_key'] === 'TRUE') {
                $row['constraint_type'] = 'Primary Key';
            } elseif ($row['is_unique_key'] === 'TRUE') {
                $row['constraint_type'] = 'Unique Key';
            }
            unset($row['is_primary_key'], $row['is_unique_key']);
            $constraints[$ts][$tn][$cn] = $row;
        }

        $sql = <<<EOD
SELECT constraint_name, schema_name AS table_schema, table_name, column_name,
    referenced_schema_name AS referenced_table_schema, referenced_table_name, referenced_column_name,
    update_rule, delete_rule
FROM SYS.REFERENTIAL_CONSTRAINTS WHERE schema_name IN ('{$schema}')
EOD;
        $result = $this->connection->select($sql);
        foreach ($result as $row) {
            $row = array_change_key_case((array)$row, CASE_LOWER);
            $ts = strtolower($row['table_schema']);
            $tn = strtolower($row['table_name']);
            $cn = strtolower($row['constraint_name']);
            $row['constraint_type'] = 'Foreign Key';
            $constraints[$ts][$tn][$cn] = $row;
        }

        return $constraints;
    }

    public function getSchemas()
    {
        Log::debug('getSchemas');

        $sql = <<<MYSQL
SELECT SCHEMA_NAME FROM SYS.SCHEMAS WHERE HAS_PRIVILEGES = 'TRUE'
MYSQL;

        return $this->selectColumn($sql);
    }

    /**
     * @inheritdoc
     */
    protected function getTableNames($schema = '')
    {
        $params = [];
        $sql = "SELECT SCHEMA_NAME, TABLE_NAME FROM SYS.M_TABLES";

        if (!empty($schema)) {
            $sql .= " WHERE SCHEMA_NAME = '{$schema}'";
            $params[] = $schema;
        }

        $sql .= " ORDER BY TABLE_NAME";

        Log::debug("Final SQL: " . $sql);
        Log::debug("Params: " . json_encode($params));

        // Avoid automatic substitution if DF is doing something weird
        if (!empty($params)) {
            $rows = $this->connection->select($sql);
        } else {
            $rows = $this->connection->select($sql);
        }

        $names = [];
        foreach ($rows as $row) {
            $row = array_change_key_case((array)$row, CASE_LOWER);
            $schemaName = $row['schema_name'] ?? '';
            $resourceName = $row['table_name'] ?? '';
            $internalName = $schemaName . '.' . $resourceName;
            $name = $resourceName;
            $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);
            $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
            $settings['description'] = null;
            $names[strtolower($name)] = new TableSchema($settings);
        }

        return $names;
    }

    /**
     * @inheritdoc
     */
    protected function getViewNames($schema = '')
    {
        Log::debug('getViewNames');

        $sql = "SELECT schema_name, view_name FROM sys.views";
        if (!empty($schema)) {
            $safeSchema = addslashes($schema); // escape just in case
            $sql .= " WHERE schema_name = '{$safeSchema}'";
        }

        $sql .= " ORDER BY view_name";

        Log::debug("Final SQL: {$sql}");

        $rows = $this->connection->select($sql); // No param binding

        $names = [];
        foreach ($rows as $row) {
            $row = array_change_key_case((array)$row, CASE_LOWER);
            $schemaName = $row['schema_name'] ?? '';
            $resourceName = $row['view_name'] ?? '';
            $internalName = $schemaName . '.' . $resourceName;
            $name = $resourceName;
            $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);
            $settings = compact('schemaName', 'resourceName', 'name', 'internalName', 'quotedName');
            $settings['isView'] = true;
            $settings['description'] = null; // sys.views likely doesn't have 'comments'
            $names[strtolower($name)] = new TableSchema($settings);
        }

        return $names;
    }


    /**
     * @inheritdoc
     */
    public function getProcedureNames($schema = '')
    {
        $bindings = [];
        $where = '';
        if (!empty($schema)) {
            $where = 'WHERE schema_name = ?';
            $bindings[] = $schema;
        }
        Log::debug('getProcedureNames');

        $sql = <<<MYSQL
SELECT procedure_name FROM SYS.PROCEDURES {$where} ORDER BY procedure_name
MYSQL;

        $rows = $this->selectColumn($sql, $bindings);
        $names = [];
        foreach ($rows as $resourceName) {
            $schemaName = $schema;
            $internalName = $schemaName . '.' . $resourceName;
            $name = $resourceName;
            $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);
            $settings = compact('schemaName', 'resourceName', 'name', 'quotedName', 'internalName');
            $names[strtolower($name)] = new ProcedureSchema($settings);
        }

        return $names;
    }

    public function getFunctionNames($schema = '')
    {
        $bindings = [];
        $where = '';
        if (!empty($schema)) {
            $where = 'WHERE schema_name = ?';
            $bindings[] = $schema;
        }
        Log::debug('getFunctionNames');

        $sql = <<<MYSQL
SELECT function_name FROM SYS.FUNCTIONS {$where} ORDER BY function_name
MYSQL;

        $rows = $this->selectColumn($sql, $bindings);

        $names = [];
        foreach ($rows as $resourceName) {
            $schemaName = $schema;
            $internalName = $schemaName . '.' . $resourceName;
            $name = $resourceName;
            $quotedName = $this->quoteTableName($schemaName) . '.' . $this->quoteTableName($resourceName);
            $settings = compact('schemaName', 'resourceName', 'name', 'quotedName', 'internalName');
            $names[strtolower($name)] = new FunctionSchema($settings);
        }

        return $names;
    }

    /**
     * Loads the parameter metadata for the specified stored procedure or function.
     *
     * @param RoutineSchema $holder
     */
    protected function loadParameters(RoutineSchema $holder)
    {
        Log::debug('loadParameters');

        $sql = <<<MYSQL
SELECT 
    parm_id, parmmode, parmname, parmtype, parmdomain, user_type, length, scale, "default"
FROM 
    SYS.SYSPROCPARMS
WHERE 
    procname = '{$holder->resourceName}' AND schema_name = '{$holder->schemaName}'
MYSQL;

        foreach ($this->connection->select($sql) as $row) {
            $row = array_change_key_case((array)$row, CASE_LOWER);
            $simpleType = static::extractSimpleType(array_get($row, 'parmdomain'));
            /*
            parmtype	SMALLINT	The type of parameter will be one of the following:
            0 - Normal parameter (variable)
            1 - Result variable - used with a procedure that returns result sets
            2 - SQLSTATE error value
            3 - SQLCODE error value
            4 - Return value from function
             */
            switch (intval(array_get($row, 'parmtype'))) {
                case 0:
                    $holder->addParameter(new ParameterSchema(
                        [
                            'name'          => array_get($row, 'parmname'),
                            'position'      => intval(array_get($row, 'parm_id')),
                            'param_type'    => array_get($row, 'parmmode'),
                            'type'          => $simpleType,
                            'db_type'       => array_get($row, 'parmdomain'),
                            'length'        => (isset($row['length']) ? intval(array_get($row, 'length')) : null),
                            'precision'     => (isset($row['length']) ? intval(array_get($row, 'length')) : null),
                            'scale'         => (isset($row['scale']) ? intval(array_get($row, 'scale')) : null),
                            'default_value' => array_get($row, 'default'),
                        ]
                    ));
                    break;
                case 1:
                case 4:
                    $holder->returnType = $simpleType;
                    break;
            }
        }
    }

    /**
     * Builds a SQL statement for renaming a DB table.
     *
     * @param string $table   the table to be renamed. The name will be properly quoted by the method.
     * @param string $newName the new table name. The name will be properly quoted by the method.
     *
     * @return string the SQL statement for renaming a DB table.
     */
    public function renameTable($table, $newName)
    {
        return "sp_rename '$table', '$newName'";
    }

    /**
     * Builds a SQL statement for renaming a column.
     *
     * @param string $table   the table whose column is to be renamed. The name will be properly quoted by the method.
     * @param string $name    the old name of the column. The name will be properly quoted by the method.
     * @param string $newName the new name of the column. The name will be properly quoted by the method.
     *
     * @return string the SQL statement for renaming a DB column.
     */
    public function renameColumn($table, $name, $newName)
    {
        return "sp_rename '$table.$name', '$newName', 'COLUMN'";
    }

    /**
     * Builds a SQL statement for changing the definition of a column.
     *
     * @param string $table      the table whose column is to be changed. The table name will be properly quoted by the
     *                           method.
     * @param string $column     the name of the column to be changed. The name will be properly quoted by the method.
     * @param string $definition the new column type. The {@link getColumnType} method will be invoked to convert
     *                           abstract column type (if any) into the physical one. Anything that is not recognized
     *                           as abstract type will be kept in the generated SQL. For example, 'string' will be
     *                           turned into 'varchar(255)', while 'string not null' will become 'varchar(255) not
     *                           null'.
     *
     * @return string the SQL statement for changing the definition of a column.
     * @since 1.1.6
     */
    public function alterColumn($table, $column, $definition)
    {
        Log::debug('alterColumn');

        $sql = <<<MYSQL
ALTER TABLE $table ALTER COLUMN {$this->quoteColumnName($column)} {$this->getColumnType($definition)}
MYSQL;

        return $sql;
    }

    public function typecastToClient($value, $field_info, $allow_null = true)
    {
        if (' ' === $value) {
            // SQL Anywhere strangely returns empty string as a single space string
            $value = '';
        }

        return parent::typecastToClient($value, $field_info, $allow_null);
    }

    /**
     * Extracts the PHP type from DB type.
     *
     * @param ColumnSchema $column
     * @param string       $dbType DB type
     */
    public function extractType(ColumnSchema $column, $dbType)
    {
        parent::extractType($column, $dbType);

        $simpleType = strstr($dbType, '(', true);
        $simpleType = strtolower($simpleType ?: $dbType);

        switch ($simpleType) {
            case 'long varchar':
                $column->type = DbSimpleTypes::TYPE_TEXT;
                break;
            case 'long nvarchar':
                $column->type = DbSimpleTypes::TYPE_TEXT;
                $column->supportsMultibyte = true;
                break;
        }
    }

    /**
     * Extracts the default value for the column.
     * The value is typecasted to correct PHP type.
     *
     * @param ColumnSchema $field
     * @param mixed        $defaultValue the default value obtained from metadata
     */
    public function extractDefault(ColumnSchema $field, $defaultValue)
    {
        if ('autoincrement' === $defaultValue) {
            $field->defaultValue = null;
            $field->autoIncrement = true;
        } elseif (('(NULL)' === $defaultValue) || ('' === $defaultValue)) {
            $field->defaultValue = null;
        } elseif ($field->type === DbSimpleTypes::TYPE_BOOLEAN) {
            if ('1' === $defaultValue) {
                $field->defaultValue = true;
            } elseif ('0' === $defaultValue) {
                $field->defaultValue = false;
            } else {
                $field->defaultValue = null;
            }
        } elseif ($field->type === DbSimpleTypes::TYPE_TIMESTAMP) {
            $field->defaultValue = null;
            if ('current timestamp' === $defaultValue) {
                $field->defaultValue = ['expression' => 'CURRENT TIMESTAMP'];
                $field->type = DbSimpleTypes::TYPE_TIMESTAMP_ON_CREATE;
            } elseif ('timestamp' === $defaultValue) {
                $field->defaultValue = ['expression' => 'TIMESTAMP'];
                $field->type = DbSimpleTypes::TYPE_TIMESTAMP_ON_UPDATE;
            }
        } else {
            parent::extractDefault($field, str_replace(['(', ')', "'"], '', $defaultValue));
        }
    }

    /**
     * Extracts size, precision and scale information from column's DB type.
     * We do nothing here, since sizes and precisions have been computed before.
     *
     * @param ColumnSchema $field
     * @param string       $dbType the column's DB type
     */
    public function extractLimit(ColumnSchema $field, $dbType)
    {
    }

    /**
     * @inheritdoc
     */
    protected function getProcedureStatement(RoutineSchema $routine, array $param_schemas, array &$values)
    {
        // Note that using the dblib driver doesn't allow binding of output parameters,
        // and also requires declaration prior to and selecting after to retrieve them.

        $paramStr = '';
        $prefix = '';
        $postfix = '';
        $bindings = [];
        foreach ($param_schemas as $key => $paramSchema) {
            switch ($paramSchema->paramType) {
                case 'IN':
                    $pName = ':' . $paramSchema->name;
                    $paramStr .= (empty($paramStr)) ? $pName : ", $pName";
                    $bindings[$pName] = array_get($values, $key);
                    break;
                case 'INOUT':
//                    $pName = $paramSchema->name;
//                    $paramStr .= (empty($paramStr) ? $pName : ", $pName");
                    // with dblib driver you can't bind output parameters
//                    $prefix .= "CREATE VARIABLE $pName {$paramSchema->dbType};";
//                    $prefix .= "SET $pName = " . array_get($values, $paramSchema->name) . ';';
//                    $postfix .= "SELECT $pName as " . $this->quoteColumnName($paramSchema->name) . ';';
                    break;
                case 'OUT':
//                    $pName = $paramSchema->name;
//                    $paramStr .= (empty($paramStr) ? $pName : ", $pName");
                    // with dblib driver you can't bind output parameters
//                    $prefix .= "CREATE VARIABLE $pName {$paramSchema->dbType};";
//                    $postfix .= "SELECT $pName as " . $this->quoteColumnName($paramSchema->name) . ';';
                    break;
            }
        }

        return "$prefix CALL {$routine->quotedName}($paramStr); $postfix";
    }

    protected function doRoutineBinding($statement, array $paramSchemas, array &$values)
    {
        // Note that using the dblib driver doesn't allow binding of output parameters,
        // and also requires declaration prior to and selecting after to retrieve them.
        $dblib = in_array('dblib', \PDO::getAvailableDrivers());
        // do binding
        foreach ($paramSchemas as $key => $paramSchema) {
            switch ($paramSchema->paramType) {
                case 'IN':
                    $this->bindValue($statement, ':' . $paramSchema->name, array_get($values, $key));
                    break;
                case 'INOUT':
                case 'OUT':
                    if (!$dblib) {
                        $pdoType = $this->extractPdoType($paramSchema->type);
                        $this->bindParam($statement, ':' . $paramSchema->name, $values[$key],
                            $pdoType | \PDO::PARAM_INPUT_OUTPUT, $paramSchema->length);
                    }
                    break;
            }
        }
    }
}
