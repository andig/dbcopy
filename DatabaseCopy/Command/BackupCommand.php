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

	protected $batch;			// batch size
	protected $constraints;

	protected function configure() {
		$this->setName('backup')
			->setDescription('Run backup')
			->addOption('config', 'c', InputOption::VALUE_REQUIRED, 'Config file')
	 		->addOption('batch', 'b', InputOption::VALUE_REQUIRED, 'Batch size')
	 		->addOption('keep-constraints', 'k', InputOption::VALUE_NONE, 'Keep constraints - avoid dropping foreign keys')
	 		->addArgument('tables', InputArgument::OPTIONAL | InputArgument::IS_ARRAY, 'Table(s)');
	}

	protected function getSimplePK($table) {
		$columns = $table->getPrimaryKey()->getColumns();

		if (sizeof($columns) !== 1)
			throw new \Exception('Table ' . $table->getName() . ' doesn\'t have a simple primary key');

		return $columns[0];
	}

	protected function validateTableExists($sm, $name) {
		if (!in_array($name, $this->getTables($sm)))
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
		// count selection range
		$sqlCount = 'SELECT COUNT(1) FROM (' . $this->sc->quoteIdentifier($table->getName()) . ')';
		if ($keyColumn && isset($maxKey)) {
			$sqlCount .= ' WHERE ' . $this->sc->quoteIdentifier($keyColumn) . ' > ?';
		}
		$totalRows = $this->sc->fetchColumn($sqlCount, $sqlParameters);
		echo($totalRows . " rows (" . (($keyColumn) ? 'partial copy' : 'overwrite') . ")\n");

		// set selection range
		if ($keyColumn) {
			$sqlMax = 'SELECT MAX(' . $this->tc->quoteIdentifier($keyColumn) . ') ' .
					  'FROM ' . $this->tc->quoteIdentifier($table->getName());
			$maxKey = $this->tc->fetchColumn($sqlMax);

			if (isset($maxKey))
				$sqlParameters[] = $maxKey;
		}
		else {
			// clear target table
			$this->truncateTable($this->tc, $table);
		}

		$progress = new ProgressBar($this->output, $totalRows);
		$progress->setFormatDefinition('debug', ' [%bar%] %percent:3s%% %elapsed:8s%/%estimated:-8s% %current% rows');
		$progress->setFormat('debug');

		$freq = (int)$totalRows / 20;
		$progress->setRedrawFrequency(($freq > 10) ? $freq : 10);

		$progress->start();

		// transfer sql
		$sqlValuePlaceholder = '(' . join(array_fill(0, sizeof($table->getColumns()), '?'), ',') . ')';
		$loopOffsetIndex = 0;

		do {
			// get source data
			$sql = 'SELECT ' . $columns . ' FROM ' . $this->sc->quoteIdentifier($table->getName());

			// limit selection for PRIMARY KEY mode
			if ($keyColumn) {
				if (isset($maxKey)) {
					$sql .= ' WHERE ' . $this->sc->quoteIdentifier($keyColumn) . ' > ?';
					$sqlParameters = array($maxKey);
				}

				$sql .= ' ORDER BY ' . $this->sc->quoteIdentifier($keyColumn) .
						' LIMIT ' . $this->batch;
			}
			else {
				$sql .= ' LIMIT ' . $this->batch . ' OFFSET ' . ($this->batch * $loopOffsetIndex++);
			}

			if (sizeof($rows = $this->sc->fetchAll($sql, $sqlParameters)) == 0) {
				// avoid div by zero in progress->advance
				break;
			}

			if ($this->tc->getDatabasePlatform()->getName() !== 'sqlite') {
				$sqlInsert =
					'INSERT INTO ' . $this->tc->quoteIdentifier($table->getName()) . ' (' . $columns . ') ' .
					'VALUES ' . join(array_fill(0, count($rows), $sqlValuePlaceholder), ',');

			    $data = array();
				foreach ($rows as $row) {
					// remember max key
					if ($keyColumn)
						$maxKey = $row[$keyColumn];

					$data = array_merge($data, array_values($row));
				}

				$this->tc->executeUpdate($sqlInsert, $data);
				$progress->advance(count($rows));
			}
			else {
				// sqlite
				$stmt = $this->tc->prepare(
					'INSERT INTO ' . $this->tc->quoteIdentifier($table->getName()) . ' (' . $columns . ') ' .
					'VALUES (' . join(array_fill(0, count($table->getColumns()), '?'), ',') . ')'
				);

				$this->tc->beginTransaction();
				foreach ($rows as $row) {
					// remember max key
					if ($keyColumn)
						$maxKey = $row[$keyColumn];

					$stmt->execute(array_values($row));
					$progress->advance(1);
				}
				$this->tc->commit();
			}
		}
		while ($keyColumn && sizeof($rows));

		$progress->finish();
		echo("\n\n"); // CRLF after progressbar @ 100%
	}

	protected function execute(InputInterface $input, OutputInterface $output) {
		parent::execute($input, $output);

		$this->batch = $input->getOption('batch') ?: self::BATCH_SIZE;

		// make sure schemas exists
		$sm = $this->sc->getSchemaManager();
		$this->validateSchema($this->getConfig('source'));

		$tm = $this->tc->getSchemaManager();
		$this->validateSchema($this->getConfig('target'));

		if (!$input->getOption('keep-constraints')) {
			$this->dropConstraints($tm, $this->getConfig('tables'));
		}

		if (($tables = $this->getOptionalConfig('tables')) == null) {
			echo("tables not configured - discovering from source schema\n");
			$tables = array();

			foreach ($this->getTables($sm) as $tableName) {
				echo($tableName . " discovered\n");
				$tables[] = array(
					'name' => $tableName,
					'mode' => self::MODE_COPY
				);
			}
		}

		foreach ($tables as $workItem) {
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

		if (!$input->getOption('keep-constraints')) {
			$this->addConstraints($tm);
		}
	}
}

?>
