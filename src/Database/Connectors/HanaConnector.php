<?php

namespace DreamFactory\Core\Hana\Database\Connectors;

use DreamFactory\Core\Hana\Pdo\PdoOdbc;
use Illuminate\Database\Connectors\Connector;
use Illuminate\Database\Connectors\ConnectorInterface;
use Exception;
use PDO;

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
        $options = $this->getOptions($config);

        return $this->createConnection($dsn, $config, $options);
    }

    public function createConnection($dsn, array $config, array $options)
    {
        list($username, $password) = [
            $config['username'] ?? null, $config['password'] ?? null,
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
     * Create a DSN string from a configuration.
     *
     * @param  array $config
     * @return string
     */
    protected function getDsn(array $config)
    {
        extract($config, EXTR_SKIP);

        $dsn = "odbc:";

        if (empty($odbc_driver)) {
            $odbc_driver = "/usr/sap/hdbclient/libodbcHDB.so";
        }
        $dsn .= "Driver={$odbc_driver};";
        if (!empty($host)) {
            $dsn .= "ServerNode={$host}";
            if (!empty($port)) {
                $dsn .= ":{$port};";
            } else {
                $dsn .= ';';
            }
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


        return $dsn;
    }
}
