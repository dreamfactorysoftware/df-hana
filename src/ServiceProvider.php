<?php

namespace DreamFactory\Core\Hana;

use DreamFactory\Core\Components\DbSchemaExtensions;
use DreamFactory\Core\Enums\LicenseLevel;
use DreamFactory\Core\Enums\ServiceTypeGroups;
use DreamFactory\Core\Services\ServiceManager;
use DreamFactory\Core\Services\ServiceType;
use DreamFactory\Core\Hana\Database\Connectors\HanaConnector;
use DreamFactory\Core\Hana\Database\Schema\HanaSchema;
use DreamFactory\Core\Hana\Database\HanaConnection;
use DreamFactory\Core\Hana\Models\HanaDbConfig;
use DreamFactory\Core\Hana\Services\Hana;
use Illuminate\Database\DatabaseManager;

class ServiceProvider extends \Illuminate\Support\ServiceProvider
{
    public function register()
    {
        // Add our database drivers.
        $this->app->resolving('db', function (DatabaseManager $db) {
            $db->extend('hana', function ($config) {
                $connector = new HanaConnector();
                $connection = $connector->connect($config);

                return new HanaConnection($connection, $config["database"], $config["prefix"], $config);
            });
        });

        // Add our service types.
        $this->app->resolving('df.service', function (ServiceManager $df) {
            $df->addType(
                new ServiceType([
                    'name'                  => 'hana',
                    'label'                 => 'SAP HANA',
                    'description'           => 'Database service supporting SAP HANA connections.',
                    'group'                 => ServiceTypeGroups::DATABASE,
                    'subscription_required' => LicenseLevel::SILVER,
                    'config_handler'        => HanaDbConfig::class,
                    'factory'               => function ($config) {
                        return new Hana($config);
                    },
                ])
            );
        });

        // Add our database extensions.
        $this->app->resolving('db.schema', function (DbSchemaExtensions $db) {
            $db->extend('hana', function ($connection) {
                return new HanaSchema($connection);
            });
        });
    }
}
