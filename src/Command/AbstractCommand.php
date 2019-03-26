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

    use ConfigTrait;

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
		$this->loadConfig($input);

		$this->sc = \Doctrine\DBAL\DriverManager::getConnection($this->getConfig('source'));
		$this->tc = \Doctrine\DBAL\DriverManager::getConnection($this->getConfig('target'));

		// make sure all connections are UTF8
		if ($this->sc->getDatabasePlatform()->getName() == 'mysql') {
			$this->sc->executeQuery("SET NAMES utf8");
		}
		if ($this->tc->getDatabasePlatform()->getName() == 'mysql') {
			$this->tc->executeQuery("SET NAMES utf8");
		}
	}
}

?>
