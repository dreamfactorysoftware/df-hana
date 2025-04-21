<?php

namespace DreamFactory\Core\Hana\Services;

use DreamFactory\Core\SqlDb\Resources\StoredFunction;
use DreamFactory\Core\SqlDb\Resources\StoredProcedure;
use DreamFactory\Core\SqlDb\Services\SqlDb;
use Illuminate\Support\Arr;
use Log;

/**
 * Class Hana
 *
 * @package DreamFactory\Core\Hana\Services
 */
class Hana extends SqlDb
{
    public static function adaptConfig(array &$config)
    {
        $config['driver'] = 'odbc';
        $driverPath = env('HANA_SERVER_ODBC_DRIVER_PATH', '/usr/sap/hdbclient/libodbcHDB.so');
        if (isset($config['options']['driver_path'])) {
            $driverPath = $config['options']['driver_path'];
        }
        $config['dsn'] =
            "Driver={$driverPath};"
            . "ServerNode={$config['host']}:{$config['port']};"
            . "DatabaseName={$config['database']};"
            . "UID={$config['username']};"
            . "PWD={$config['password']};";
        Log::debug('HanaConnector DSN:' . $config['dsn']);

        parent::adaptConfig($config);
    }

    // Hide _schema endpoints and related parameter
    public function getApiDocInfo()
    {
        $base = parent::getApiDocInfo();
        $paths = (array)Arr::get($base, 'paths');
        foreach ($paths as $path_key => $path) {
            if (str_contains($path_key, '_schema')) {
                unset($paths[$path_key]);
                continue;
            }

        }
        $base['paths'] = $paths;
        return $base;
    }

    public function getResourceHandlers()
    {
        $handlers = parent::getResourceHandlers();

        $handlers[StoredProcedure::RESOURCE_NAME] = [
            'name'       => StoredProcedure::RESOURCE_NAME,
            'class_name' => StoredProcedure::class,
            'label'      => 'Stored Procedure',
        ];
        $handlers[StoredFunction::RESOURCE_NAME] = [
            'name'       => StoredFunction::RESOURCE_NAME,
            'class_name' => StoredFunction::class,
            'label'      => 'Stored Function',
        ];

        return $handlers;
    }
}
