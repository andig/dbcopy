<?php

namespace DatabaseCopy\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputDefinition;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class ClearCommand extends AbstractCommand {

	protected function configure() {
		$this->setName('clear')
			->setDescription('Clear target tables')
			->addOption('config', 'c', InputOption::VALUE_REQUIRED, 'Config file')
			->addOption('drop', 'd', InputOption::VALUE_NONE, 'Drop target table instead of truncating');
	}

	protected function dropTable($conn, $table) {
		// 'DROP TABLE ' . $conn->quoteIdentifier($table->getName())
		$sql = $conn->getDatabasePlatform()->getDropTableSQL($table->getName());
		$conn->executeQuery($sql);
	}

	protected function tableExists($sm, $tableName) {
		$tables = array_map(function($table) {
			return $table->getname();
		}, $sm->listTables());

		return in_array($tableName, $tables);
	}

	protected function execute(InputInterface $input, OutputInterface $output) {
		parent::execute($input, $output);

		// make sure schemas exists
		$tm = $this->tc->getSchemaManager();
		$this->validateSchema($this->getConfig('target'));
		// $this->dropConstraints($tm, $this->getConfig('tables'));
		$this->tc->beginTransaction();

		foreach ($this->getConfig('tables') as $name => $mode) {
			// check if table exists
			if (!$this->tableExists($tm, $name)) {
				echo($name . " doesn't exist - skipping.\n");
				continue;
			}

			$table = $tm->listTableDetails($name);

			if ($input->getOption('drop')) {
				echo($name . ": dropping\n");
				$this->dropTable($this->tc, $table);
			}
			else {
				echo($name . ": truncating\n");
				$this->truncateTable($this->tc, $table);
			}
		}

		$this->tc->commit();
	}
}

?>
