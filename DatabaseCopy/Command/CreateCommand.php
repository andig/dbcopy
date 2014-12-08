<?php

namespace DatabaseCopy\Command;

use Doctrine\DBAL\Schema\Synchronizer\SingleDatabaseSynchronizer;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputDefinition;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;


if (!function_exists('array_column')) {

	/**
	 * Simplified array_column polyfill
	 */
	function array_column($ary, $columnKey)
	{
	    return array_map(create_function('&$row', 'return $row["'.$columnKey.'"];'), $ary);
	}
}

class CreateCommand extends AbstractCommand {

	protected $indexes;	// names assets

	protected function configure() {
		$this->setName('create')
			->setDescription('Create target schema')
			->addOption('config', 'c', InputOption::VALUE_REQUIRED, 'Config file');
	}

	/**
	 * Rename indexes to avoid duplicate name conflicts
	 */
	protected function renameIndexes($table) {
		// rename indexes
		foreach ($table->getIndexes() as $idx) {
			// primary index doesn't need renaming
			if ($idx->isPrimary())
				continue;

			// echo("idx: " . $idx->getName() . "\n");

			if (in_array($idx->getName(), $this->indexes)) {
				echo("duplicate index: " . $idx->getName() . "\n");

				$old = $idx;
				$columns = $old->getColumns();

				// create new index
				if ($old->isUnique())
					$idx = $table->addUniqueIndex($columns);
				else
					$idx = $table->addIndex($columns);

				// drop old index
				$table->dropIndex($old->getName());

				echo("renaming: " . $idx->getName() . "\n");
			}

			$this->indexes[] = $idx->getName();
		}
	}

	protected function renameAssets($schema, $platform) {
		$this->indexes = array();

		echo("Renaming assets for target platform: " . $platform . "\n");

		foreach ($schema->getTables() as $table) {
			echo("table: " . $table->getName() . "\n");

			// foreach($table->getForeignKeys() as $fk) {
			// 	echo("fk: " . $fk->getName() . "\n");
			// }

			switch ($platform) {
				case 'sqlite':
					$this->renameIndexes($table);
					break;
			}
		}
	}

	protected function execute(InputInterface $input, OutputInterface $output) {
		parent::execute($input, $output);

		// make sure schemas exists
		$sm = $this->sc->getSchemaManager();
		$this->validateSchema($this->getConfig('source'));

		echo("Creating target schema\n");

		// create schema if it doesn't exist
		if ((($db = $this->getOptionalConfig('target.dbname')) !== null) &&
			$this->validateSchema($this->getConfig('target'), false) == false) {

			// get schema-less connection
			$_config = $this->getConfig('target');
			unset($_config['dbname']);

			$conn = \Doctrine\DBAL\DriverManager::getConnection($_config);
			$tm = $conn->getSchemaManager();

			echo("Creating database\n");
			$tm->createDatabase($db);
		}

		try {
			$schema = $sm->createSchema();

			// make sure schema is free of conflicts for target platform
			if (in_array($platform = $this->tc->getDatabasePlatform()->getName(), array('sqlite'))) {
				$this->renameAssets($schema, $platform);
			}

			echo("Creating tables\n");

			// sync configured tables only
			if (($tables = $this->getOptionalConfig('tables'))) {
				// extract target table names
				$tables = array_column($tables, 'name');

				foreach ($schema->getTables() as $table) {
					if (!in_array($table->getName(), $tables)) {
						$schema->dropTable($table->getName());
					}
				}
			}

			$synchronizer = new SingleDatabaseSynchronizer($this->tc);
			$synchronizer->createSchema($schema);
		}
		catch (\Exception $e) {
			$sql = $schema->toSql($this->tc->getDatabasePlatform());
			echo(join($sql, "\n"));
			throw $e;
		}
	}
}

?>
