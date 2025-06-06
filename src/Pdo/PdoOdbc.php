<?php

namespace DreamFactory\Core\Hana\Pdo;

use PDO;
use DreamFactory\Core\Hana\Pdo\Odbc\Exceptions\PdoOdbcException;
use DreamFactory\Core\Hana\Pdo\Odbc\Statement;
use Log;
/**
 * Oci8 class to mimic the interface of the PDO class
 * This class extends PDO but overrides all of its methods. It does this so
 * that instanceof checks and type-hinting of existing code will work
 * seamlessly.
 */
class PdoOdbc extends PDO
{
    /**
     * Database handler
     *
     * @var resource
     */
    private $dbh;

    /**
     * Driver options.
     *
     * @var array
     */
    private $options = [];

    /**
     * Whether currently in a transaction.
     *
     * @var bool
     */
    private $inTransaction = false;

    /**
     * Insert query statement table variable.
     *
     * @var string
     */
    private $table;

    /**
     * Creates a PDO instance representing a connection to a database.
     *
     * @param string $dsn
     * @param string $username
     * @param string $password
     * @param array  $options
     * @throws PdoOdbcException
     */
    public function __construct($dsn, $username, $password, array $options = [])
    {
        $charset = null;
        $dsn = preg_replace('/^odbc:/', '', $dsn);
        $tokens = preg_split('/;/', $dsn);
//        $dsn     = str_replace(['dbname=//', 'dbname='], '', $tokens[0]);

        //Find the charset in Connection String: oci:dbname=192.168.10.145/orcl;charset=CL8MSWIN1251
        $charset = $this->_getCharset($tokens);
        // OR Get charset from options
        if (!$charset) {
            $charset = $this->configureCharset($options);
        }

        $this->connect($dsn, $username, $password, $options, $charset);

        // Save the options
        $this->options = $options;
    }

    public function getConnection()
    {
        return $this->dbh;
    }

    /**
     * Prepares a statement for execution and returns a statement object
     *
     * @param string $statement This must be a valid SQL statement for the
     *                          target database server.
     * @param array  $options   [optional] This array holds one or more key=>value
     *                          pairs to set attribute values for the PDOStatement object that this
     *                          method returns.
     * @throws PdoOdbcException
     * @return Statement
     */
    public function prepare($statement, $options = null)
    {
        // Get instance options
        if ($options == null) {
            $options = $this->options;
        }

        // check if statement is insert function
        if (stripos($statement, 'insert into') !== false) {
            preg_match('/insert into\s+([^\s\(]*)?/i', $statement, $matches);
            // store insert into table name
            $this->table = $matches[1];
        }

        // Prepare the statement
        $sth = odbc_prepare($this->dbh, $statement);

        if (!$sth) {
            $e = odbc_error($this->dbh);
            throw new PdoOdbcException($e['message']);
        }

        if (!is_array($options)) {
            $options = [];
        }

        return new Statement($sth, $this, $options);
    }

    /**
     * Initiates a transaction.
     *
     * @throws PdoOdbcException
     * @return bool TRUE on success or FALSE on failure
     */
    public function beginTransaction()
    {
        if ($this->inTransaction()) {
            throw new PdoOdbcException('There is already an active transaction');
        }

        $this->inTransaction = true;

        return true;
    }

    /**
     * Returns true if the current process is in a transaction.
     *
     * @deprecated Use inTransaction() instead
     * @return bool
     */
    public function isTransaction()
    {
        return $this->inTransaction();
    }

    /**
     * Checks if inside a transaction.
     *
     * @return bool TRUE if a transaction is currently active, and FALSE if not.
     */
    public function inTransaction()
    {
        return $this->inTransaction;
    }

    /**
     * Commits a transaction.
     *
     * @return bool TRUE on success or FALSE on failure.
     */
    public function commit()
    {
        if (odbc_commit($this->dbh)) {
            $this->inTransaction = false;

            return true;
        }

        return false;
    }

    /**
     * Rolls back a transaction.
     *
     * @throws PdoOdbcException
     * @return bool TRUE on success or FALSE on failure.
     */
    public function rollBack()
    {
        if (!$this->inTransaction()) {
            throw new PdoOdbcException('There is no active transaction');
        }

        if (odbc_rollback($this->dbh)) {
            $this->inTransaction = false;

            return true;
        }

        return false;
    }

    /**
     * Sets an attribute on the database handle.
     *
     * @param int   $attribute
     * @param mixed $value
     * @return bool TRUE on success or FALSE on failure.
     */
    public function setAttribute($attribute, $value)
    {
        $this->options[$attribute] = $value;

        return true;
    }

    /**
     * Executes an SQL statement and returns the number of affected rows.
     *
     * @param string $statement The SQL statement to prepare and execute.
     * @return int The number of rows that were modified or deleted by the SQL
     *                          statement you issued.
     */
    public function exec($statement)
    {
        $stmt = $this->prepare($statement);
        $stmt->execute();

        return $stmt->rowCount();
    }

    /**
     * Executes an SQL statement, returning the results as a
     * Yajra\Pdo\Oci8\Statement object.
     *
     * @param string     $statement The SQL statement to prepare and execute.
     * @param int|null   $fetchMode The fetch mode must be one of the
     *                              PDO::FETCH_* constants.
     * @param mixed|null $modeArg   Column number, class name or object.
     * @param array|null $ctorArgs  Constructor arguments.
     * @return Statement
     */
    public function query(string $statement, ?int $fetchMode = null, ...$fetchModeArgs)
    {
        // Prepare the statement
        $stmt = $this->prepare($statement);

        // Execute the prepared statement
        if (!$stmt->execute()) {
            // Handle the error if execution fails
            throw new \Exception('Query failed: ' . implode(' ', $stmt->errorInfo()));
        }

        // Set fetch mode if specified
        if ($fetchMode !== null) {
            if ($fetchMode == PDO::FETCH_CLASS) {
                // If fetching as class, include the $fetchModeArgs (class name, constructor args)
                $stmt->setFetchMode($fetchMode, ...$fetchModeArgs);
            } else {
                // Default fetch mode (e.g., FETCH_ASSOC, FETCH_OBJ)
                $stmt->setFetchMode($fetchMode);
            }
        }

        // Return the statement object (can be used to fetch results)
        return $stmt;
    }


    /**
     * returns the current value of the sequence related to the table where
     * record is inserted by default. The sequence name should follow this for it to work
     * properly:
     *   {$table}.'_id_seq'
     * If the sequence name is passed, then the function will check using that value.
     * Oracle does not support the last inserted ID functionality like MySQL.
     * If the above sequence does not exist, the method will return 0;
     *
     * @param string $sequence Sequence name
     * @return mixed Last sequence number or 0 if sequence does not exist
     */
    public function lastInsertId($sequence = null)
    {
        if (is_null($sequence)) {
            $sequence = "id";
        }

        $table = str_replace('"', '', $this->table);
        $schema = '';
        if (false !== $pos = strpos($table, '.')) {
            $schema = strstr($table, '.', true);
            $table = substr($table, $pos + 1);
        }
        $stmt = $this->query(
            "select column_id from table_columns where schema_name = '$schema' AND table_name = '$table' AND column_name = '$sequence'",
            PDO::FETCH_COLUMN
        );
        $columnId = $stmt->fetch();

        $stmt = $this->query(
            "select sequence_name from sequences where sequence_name like '%_{$columnId}_%'",
            PDO::FETCH_COLUMN
        );
        $sequenceName = $stmt->fetch();

        $stmt = $this->query(
            "SELECT SYSTEM.{$sequenceName}.CURRVAL FROM DUMMY",
            PDO::FETCH_COLUMN
        );
        $id = $stmt->fetch();

        return $id;
    }

    /**
     * Fetch the SQLSTATE associated with the last operation on the database
     * handle.
     * While this returns an error code, it merely emulates the action. If
     * there are no errors, it returns the success SQLSTATE code (00000).
     * If there are errors, it returns HY000. See errorInfo() to retrieve
     * the actual Oracle error code and message.
     *
     * @return string
     */
    public function errorCode()
    {
        $error = $this->errorInfo();

        return $error[0];
    }

    /**
     * Returns extended error information for the last operation on the database handle.
     * The array consists of the following fields:
     *   0  SQLSTATE error code (a five characters alphanumeric identifier
     *      defined in the ANSI SQL standard).
     *   1  Driver-specific error code.
     *   2  Driver-specific error message.
     *
     * @return array Error information
     */
    public function errorInfo()
    {
        $e = odbc_error($this->dbh);

        if (is_array($e)) {
            return [
                'HY000',
                $e['code'],
                $e['message']
            ];
        }

        return ['00000', null, null];
    }

    /**
     * Retrieve a database connection attribute
     *
     * @param int $attribute
     * @return mixed A successful call returns the value of the requested PDO
     *   attribute. An unsuccessful call returns null.
     */
    public function getAttribute($attribute)
    {
        if ($attribute == PDO::ATTR_DRIVER_NAME) {
            return "odbc";
        }

        if (isset($this->options[$attribute])) {
            return $this->options[$attribute];
        }

        return null;
    }

    /**
     * Special non PDO function used to start cursors in the database
     * Remember to call oci_free_statement() on your cursor
     *
     * @access public
     * @return mixed New statement handle, or FALSE on error.
     */
    public function getNewCursor()
    {
        return odbc_new_cursor($this->dbh);
    }

    /**
     * Special non PDO function used to start descriptor in the database
     * Remember to call oci_free_statement() on your cursor
     *
     * @access public
     * @param int $type One of OCI_DTYPE_FILE, OCI_DTYPE_LOB or OCI_DTYPE_ROWID.
     * @return mixed New LOB or FILE descriptor on success, FALSE on error.
     */
    public function getNewDescriptor($type = OCI_D_LOB)
    {
        return odbc_new_descriptor($this->dbh, $type);
    }

    /**
     * Special non PDO function used to close an open cursor in the database
     *
     * @access public
     * @param mixed $cursor A valid OCI statement identifier.
     * @return mixed Returns TRUE on success or FALSE on failure.
     */
    public function closeCursor($cursor)
    {
        return odbc_free_statement($cursor);
    }

    /**
     * Places quotes around the input string
     *  If you are using this function to build SQL statements, you are strongly
     * recommended to use prepare() to prepare SQL statements with bound
     * parameters instead of using quote() to interpolate user input into an SQL
     * statement. Prepared statements with bound parameters are not only more
     * portable, more convenient, immune to SQL injection, but are often much
     * faster to execute than interpolated queries, as both the server and
     * client side can cache a compiled form of the query.
     *
     * @param string $string    The string to be quoted.
     * @param int    $paramType Provides a data type hint for drivers that have
     *                          alternate quoting styles
     * @return string Returns a quoted string that is theoretically safe to pass
     *                          into an SQL statement.
     * @todo Implement support for $paramType.
     */
    public function quote($string, $paramType = PDO::PARAM_STR)
    {
        if (is_numeric($string)) {
            return $string;
        }

        return "'" . str_replace("'", "''", $string) . "'";
    }

    /**
     * Special non PDO function to check if sequence exists
     *
     * @param  string $name
     * @return boolean
     */
    public function checkSequence($name)
    {
        try {
            $stmt = $this->query(
                "SELECT count(*) FROM SEQUENCES WHERE SEQUENCE_NAME=UPPER('{$name}') AND SEQUENCE_OWNER=UPPER(USER)",
                PDO::FETCH_COLUMN
            );

            return $stmt->fetch();
        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * Check if statement can use pseudo named parameter.
     *
     * @param string $statement
     * @return bool
     */
    private function isNamedParameterable($statement)
    {
        return !preg_match('/^alter+ +table/', strtolower(trim($statement)))
            and !preg_match('/^create+ +table/', strtolower(trim($statement)));
    }

    /**
     * Connect to database.
     *
     * @param string $dsn
     * @param string $username
     * @param string $password
     * @param array  $options
     * @param string $charset
     * @throws PdoOdbcException
     */
    private function connect($dsn, $username, $password, array $options, $charset)
    {
        if (array_key_exists(PDO::ATTR_PERSISTENT, $options) && $options[PDO::ATTR_PERSISTENT]) {
            $this->dbh = odbc_pconnect($dsn, $username, $password);
        } else {
            $this->dbh = odbc_connect($dsn, $username, $password);
        }

        if (!$this->dbh) {
            $e = odbc_error();
            throw new PdoOdbcException($e['message']);
        }
    }

    /**
     * Find the charset
     *
     * @param string $charset charset
     *
     * @return charset
     */
    private function _getCharset($charset = null)
    {
        if (!$charset) {
            return null;
        }

        $expr = '/^(charset=)(\w+)$/';
        $tokens = array_filter(
            $charset, function ($token) use ($expr) {
            return preg_match($expr, $token, $matches);
        }
        );
        if (sizeof($tokens) > 0) {
            preg_match($expr, array_shift($tokens), $matches);
            $_charset = $matches[2];
        } else {
            $_charset = null;
        }

        return $_charset;
    }

    /**
     * Configure proper charset.
     *
     * @param array $options
     * @return string
     */
    private function configureCharset(array $options)
    {
        $charset = '';
        // Get the character set from the options.
        if (array_key_exists("charset", $options)) {
            $charset = $options["charset"];
        }
        // Convert UTF8 charset to AL32UTF8
        $charset = strtolower($charset) == 'utf8' ? 'AL32UTF8' : $charset;

        return $charset;
    }

    /**
     * Special non PDO function
     * Allocates new collection object
     *
     * @param string $typeName Should be a valid named type (uppercase).
     * @param string $schema   Should point to the scheme, where the named type was created.
     *                         The name of the current user is the default value.
     * @return \OCI_Collection
     */
    public function getNewCollection($typeName, $schema)
    {
        return odbc_new_collection($this->dbh, $typeName, $schema);
    }

    /**
     * Special not PDO function.
     * Get options used in creating the connection.
     *
     * @return array
     */
    public function getOptions()
    {
        return $this->options;
    }
}
