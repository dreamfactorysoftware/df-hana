<?php

namespace DreamFactory\Core\Hana\Database\Connectors;

use DreamFactory\Core\Hana\Pdo\PdoOdbc;
use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;
use Exception;
use PDO;
use Log;

class HanaConnector extends Connector implements ConnectorInterface
{
    protected $options = [
        PDO::ATTR_CASE => PDO::CASE_NATURAL,
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_ORACLE_NULLS => PDO::NULL_NATURAL,
        PDO::ATTR_STRINGIFY_FETCHES => false,
        PDO::ATTR_EMULATE_PREPARES => false,
    ];

    /**
     * Establish a database connection.
     *
     * @param  array $config
     * @return \PDO
     * @throws \Exception
     */
    public function connect(array $config)
    {
        $dsn = $this->getDsn($config);
//        Log::debug('HANA Config:', $dsn);
        $options = $this->getOptions($config);

        return $this->createConnection($dsn, $config, $options);
    }

    /**
     * Create a new PDO connection.
     *
     * @param  string $dsn
     * @param  array  $config
     * @param  array  $options
     * @return \PDO
     * @throws \Exception
     */
    public function createConnection($dsn, array $config, array $options)
    {
        [$username, $password] = [
            $config['username'] ?? null,
            $config['password'] ?? null,
        ];

        try {
            return new PdoOdbc($dsn, $username, $password, $options);
        } catch (Exception $e) {
            if ($this->causedByLostConnection($e)) {
                return new PdoOdbc($dsn, $username, $password, $options);
            }

            throw $e;
        }
    }

    /**
     * Create a DSN string from configuration.
     *
     * @param  array $config
     * @return string
     */
    protected function getDsn(array $config)
    {
        Log::debug('Handling database connection details.');
        extract($config, EXTR_SKIP);

        $dsn = 'odbc:';

        $odbc_driver = $odbc_driver ?? '/usr/sap/hdbclient/libodbcHDB.so';
        $dsn .= "Driver={$odbc_driver};";

        if (!empty($host)) {
            $dsn .= "ServerNode={$host}";
            $dsn .= !empty($port) ? ":{$port};" : ';';
        }

        if (!empty($database)) {
            $dsn .= "DATABASE={$database};";
        }

        if (!empty($username)) {
            $dsn .= "Uid={$username};";
        }

        if (!empty($password)) {
            $dsn .= "Pwd={$password};";
        }

        $dsn .= 'CHAR_AS_UTF8=true';
        Log::debug('configuring dsn: ' . $dsn);

        return $dsn;
    }
}
