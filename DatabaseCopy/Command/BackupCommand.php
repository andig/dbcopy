<?php

namespace DatabaseCopy\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputDefinition;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

use Symfony\Component\Console\Helper\ProgressBar;

class BackupCommand extends AbstractCommand {

	const MODE_COPY = 'copy';
	const MODE_SKIP = 'skip';
	const MODE_PRIMARY_KEY = 'pk';

	const BATCH_SIZE = 1000;

	protected $constraints;

	protected function configure() {
		$this->setName('backup')
			->setDescription('Run backup')
			->addOption('config', 'c', InputOption::VALUE_REQUIRED, 'Config file')
	 		->addArgument('tables', InputArgument::OPTIONAL | InputArgument::IS_ARRAY, 'Table(s)');
	}

	protected function getSimplePK($table) {
		$columns = $table->getPrimaryKey()->getColumns();

		if (sizeof($columns) !== 1)
			throw new \Exception('Table ' . $table->getName() . ' doesn\'t have a simple primary key');

		return $columns[0];
	}

	protected function validateTableExists($sm, $name) {
		$tables = array_map(function($table) {
			return $table->getname();
		}, $sm->listTables());

		if (!in_array($name, $tables))
			throw new \Exception('Table ' . $name . ' doesn\'t exist.' . "To create the schema run\n \n" .
				"	doctrine.php orm:schema-tool:create --dump-sql");
	}

	protected function dropConstraints($sm, $tables) {
		foreach ($tables as $workItem) {
			$name = $workItem['name'];
			$this->validateTableExists($sm, $name);

			$table = $sm->listTableDetails($name);

			$this->constraints = array();

			foreach ($table->getForeignKeys() as $fk) {
				$this->constraints[] = array($table, $fk);

				echo("Dropping FK " . $fk->getName() . ' on ' . $name . "\n");
				$sm->dropForeignKey($fk, $table);
			}
		}
	}

	protected function addConstraints($sm) {
		foreach ($this->constraints as $constraint) {
			list($table, $fk) = $constraint;
			echo("Creating FK " . $fk->getName() . ' on ' . $table->getName() . "\n");
			$sm->createForeignKey($fk, $table);
		}
	}

	protected function copyTable($table, $keyColumn = false) {
		$columns = join(array_map(function($column) {
			return $this->sc->quoteIdentifier($column->getName());
		}, $table->getColumns()), ',');

		$sqlParameters = array();
		$maxKey = null;

		echo($table->getName() . ": copying ");

		// set selection range
		if ($keyColumn) {
			$sqlMax = 'SELECT MAX(' . $this->tc->quoteIdentifier($keyColumn) . ') ' .
					  'FROM ' . $this->tc->quoteIdentifier($table->getName());
			$maxKey = $this->tc->fetchColumn($sqlMax);

			if (isset($maxKey))
				$sqlParameters[] = $maxKey;
		}

		// clear target table
		if ($keyColumn == false) {
			$this->truncateTable($this->tc, $table);
		}

		// count selection range
		$sqlCount = 'SELECT COUNT(1) FROM (' . $this->sc->quoteIdentifier($table->getName()) . ')';
		if ($keyColumn && isset($maxKey)) {
			$sqlCount .= ' WHERE ' . $this->sc->quoteIdentifier($keyColumn) . ' > ?';
		}

		$totalRows = $this->sc->fetchColumn($sqlCount, $sqlParameters);
		echo($totalRows . " rows (" . (($keyColumn) ? 'partial copy' : 'overwrite') . ")\n");

		// transfer sql
		$sqlInsert = 'INSERT INTO ' . $this->tc->quoteIdentifier($table->getName()) . ' (' . $columns . ') ' .
					 'VALUES (' . join(array_fill(0, sizeof($table->getColumns()), '?'), ',') . ')';

		$progress = new ProgressBar($this->output, $totalRows);
		if ($totalRows < 1000)
			$progress->setRedrawFrequency($totalRows / 4);
		elseif ($totalRows < 10000)
			$progress->setRedrawFrequency($totalRows / 10);
		else {
			$progress->setRedrawFrequency($totalRows / 20);
			$progress->setFormatDefinition('debug', ' [%bar%] %percent:3s%% %elapsed:8s%/%estimated:-8s% %current% rows');
			$progress->setFormat('debug');
		}

		$progress->start();

		do {
			// get source data
			$sql = 'SELECT ' . $columns . ' FROM ' . $this->sc->quoteIdentifier($table->getName());

			// limit selection for PRIMARY KEY mode
			if ($keyColumn) {
				if (isset($maxKey))
					$sql .= ' WHERE ' . $this->sc->quoteIdentifier($keyColumn) . ' > ?';

				$sql .= ' ORDER BY ' . $this->sc->quoteIdentifier($keyColumn) .
						' LIMIT ' . self::BATCH_SIZE;

				$sqlParameters = array($maxKey);
			}

			$rows = $this->sc->fetchAll($sql, $sqlParameters);

			// write target data
			$this->tc->beginTransaction();
			foreach ($rows as $row) {
				// remember max key
				if ($keyColumn)
					$maxKey = $row[$keyColumn];

				$this->tc->executeQuery($sqlInsert, array_values($row));
				$progress->advance();
			}
			$this->tc->commit();
		} while ($keyColumn && sizeof($rows));

		$progress->finish();
		echo("\n\n"); // CRLF after progressbar @ 100%
	}

	protected function execute(InputInterface $input, OutputInterface $output) {
		parent::execute($input, $output);

		// make sure schemas exists
		$sm = $this->sc->getSchemaManager();
		$this->validateSchema($this->getConfig('source'));

		$tm = $this->tc->getSchemaManager();
		$this->validateSchema($this->getConfig('target'));

		$this->dropConstraints($tm, $this->getConfig('tables'));

		foreach ($this->getConfig('tables') as $workItem) {
			$name = $workItem['name'];
			$mode = strtolower($workItem['mode']);

			// only selected tables?
			if ($selectedTables = $input->getArgument('tables')) {
				if (!in_array($name, $selectedTables)) continue;
			}

			$table = $sm->listTableDetails($name);

			switch ($mode) {
				case self::MODE_SKIP:
					echo($name . ": skipping\n");
					break;

				case self::MODE_COPY:
					$this->copyTable($table, false);
					break;

				case self::MODE_PRIMARY_KEY:
					$keyColumn = $this->getSimplePK($table);
					$this->copyTable($table, $keyColumn);
					break;

				default:
					throw new \Exception('Unknown mode ' . $mode);
			}
		}

		$this->addConstraints($tm);
	}
}

?>
