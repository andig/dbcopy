<?php

namespace DatabaseCopy\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputDefinition;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * AbstractCommand
 */
abstract class AbstractCommand extends Command {

	const CONFIG_FILE = 'dbcopy.json';

	protected $config;	// json configuration

	/**
	 * Strip whitespaces and comments from JSON string
	 * Nessecary for parsing a JSON string with json_decode()
	 * Borrowed from volkszaehler.org
	 *
	 * @param string $json
	 */
	private static function strip($json) {
		$json = preg_replace(array(
			// eliminate single line comments in '// ...' form
			'#//(.+)$#m',

			// eliminate multi-line comments in '/* ... */' form
			'#/\*.*?\*/#s'
		), '', $json);

		// eliminate extraneous space
		return trim($json);
	}

	protected function getConfig($keys = null, $required = true, $config = null) {
		// use $this->config if context not specified
		if ($config == null)
			$config = $this->config;

		if ($keys !== null) {
			$path = explode('.', $keys);
			foreach ($path as $key) {
				if (isset($config[$key])) {
					$config = $config[$key];
				}
				else {
					$config = null;
					break;
				}
			}
		}

		if ($required && $config == null)
			throw new \Exception('Configuration error: ' . $keys . ' is undefined at level ' . $key);

		return $config;
	}

	protected function getOptionalConfig($keys = null, $config = null) {
		return $this->getConfig($keys, false, $config);
	}

	protected function validateSchema($config, $required = true) {
		$result = null;

		// is a schema name specified?
		if (($db = $this->getOptionalConfig('dbname', $config)) !== null) {
			// create schema-less config variante - otherwise any SQL statement will fail if schema doesn't exist
			$_config = $config;
			unset($_config['dbname']);

			$conn = \Doctrine\DBAL\DriverManager::getConnection($_config);
			$sm = $conn->getSchemaManager();

			try {
				$schemas = $sm->listDatabases();

				$result = in_array($db, $schemas);

				if ($required &! $result)
					throw new \Exception('Database schema ' . $db . ' doesn\'t exist.');
			}
			catch (\Doctrine\DBAL\DBALException $e) {
				// platform->getListDatabasesSQL() may not exist on all platforms
				$result = true;
			}
		}

		return $result;
	}

	protected function truncateTable($conn, $table) {
		// 'TRUNCATE TABLE ' . $conn->quoteIdentifier($table->getName())
		$sql = $conn->getDatabasePlatform()->getTruncateTableSQL($table->getName());
		$conn->executeQuery($sql);
	}

	/**
	 * Get existing tables by name
	 */
	protected function getTables($sm) {
		$tables = array_map(function($table) {
			return $table->getname();
		}, $sm->listTables());

		return $tables;
	}

	protected function execute(InputInterface $input, OutputInterface $output) {
		$this->output = $output;

		if (($file = $input->getOption('config')) == null) {
			// current folder
			$file = getcwd() . DIRECTORY_SEPARATOR . self::CONFIG_FILE;

			if (!file_exists($file)) {
				// program folder
				$file = realpath(__DIR__) . DIRECTORY_SEPARATOR . self::CONFIG_FILE;
			}
		}

		if (($conf = @file_get_contents($file)) === false)
			throw new \Exception('Config file not found: ' . $file);

		if (($this->config = json_decode(self::strip($conf), true)) === NULL) {
			// Errordefinitionen
			$constants = get_defined_constants(true);
			$json_errors = array();
			foreach ($constants["json"] as $name => $value) {
				if (!strncmp($name, "JSON_ERROR_", 11)) {
					$json_errors[$value] = $name;
				}
			}

			throw new \Exception($json_errors[json_last_error()]);
		}

		$this->sc = \Doctrine\DBAL\DriverManager::getConnection($this->getConfig('source'));
		$this->tc = \Doctrine\DBAL\DriverManager::getConnection($this->getConfig('target'));

		// make sure all connections are UTF8
		$this->sc->executeQuery("SET NAMES utf8");
		$this->tc->executeQuery("SET NAMES utf8");
	}
}

?>
