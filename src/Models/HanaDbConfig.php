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
}