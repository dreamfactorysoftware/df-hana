<?php
namespace DreamFactory\Core\Hana\Models;

use DreamFactory\Core\SqlDb\Models\SqlDbConfig;

/**
 * HanaDbConfig
 *
 */
class HanaDbConfig extends SqlDbConfig
{
    public static function getDriverName()
    {
        return 'odbc';
    }

    public static function getDefaultPort()
    {
        return 39015;
    }

    protected function getConnectionFields()
    {
        $fields = parent::getConnectionFields();

        return array_merge($fields, ['odbc_driver']);
    }

    public static function getDefaultConnectionInfo()
    {
        $defaults = parent::getDefaultConnectionInfo();
        $defaults[] = [
            'name'        => 'odbc_driver',
            'label'       => 'ODBC Driver',
            'type'        => 'string',
            'description' => 'Optional ODBC driver name or path, defaults to /usr/sap/hdbclient/libodbcHDB.so.'
        ];

        return $defaults;
    }
}