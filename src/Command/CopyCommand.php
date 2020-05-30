<?php

namespace DatabaseCopy\Command;

use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Output\StreamOutput;
use Doctrine\Dbal\Connection;

class CopyCommand extends AbstractCommand
{
	const MODE_COPY = 'copy';
	const MODE_SKIP = 'skip';
	const MODE_PRIMARY_KEY = 'pk';

	const BATCH_SIZE = 1000;

	protected $batch;			// batch size
	protected $constraints;

	protected function configure()
	{
		$this->setName('copy')
			->setDescription('Run copy')
			->addOption('config', 'c', InputOption::VALUE_REQUIRED, 'Config file')
			->addOption('batch', 'b', InputOption::VALUE_REQUIRED, 'Batch size')
			->addOption('keep-constraints', 'k', InputOption::VALUE_NONE, 'Keep constraints - avoid dropping foreign keys')
			->addArgument('tables', InputArgument::OPTIONAL | InputArgument::IS_ARRAY, 'Table(s)')
		;
	}

	protected function getSimplePK($table)
	{
		$columns = $table->getPrimaryKey()->getColumns();

		if (1 !== sizeof($columns)) {
			throw new \Exception('Table '.$table->getName().' doesn\'t have a simple primary key');
		}

		return $columns[0];
	}

	protected function validateTableExists($sm, $name)
	{
		if (!in_array($name, $this->getTables($sm))) {
			throw new \Exception('Table '.$name.' doesn\'t exist.'."To create the schema run\n \n".
				'	doctrine.php orm:schema-tool:create --dump-sql');
		}
	}

	protected function dropConstraints($sm, $tables)
	{
		foreach ($tables as $name => $mode) {
			$this->validateTableExists($sm, $name);
			$this->constraints = [];
			$table = $sm->listTableDetails($name);

			foreach ($table->getForeignKeys() as $fk) {
				$this->constraints[] = [$table, $fk];

				echo 'Dropping FK '.$fk->getName().' on '.$name."\n";
				$sm->dropForeignKey($fk, $table);
			}
		}
	}

	protected function addConstraints($sm)
	{
		foreach ($this->constraints as $constraint) {
			list($table, $fk) = $constraint;
			echo 'Creating FK '.$fk->getName().' on '.$table->getName()."\n";
			$sm->createForeignKey($fk, $table);
		}
	}

	protected function quoted(Connection $conn, array $assets): array {
		$res = [];
		foreach ($assets as $asset) {
			$res[] = $conn->quoteIdentifier($asset->getName());
		}
		return $res;
	}

	protected function copyTable($table, $keyColumn = false)
	{
		// get quoted column names as joined string
		$sourceColumns = join($this->quoted($this->sc, $table->getColumns()), ',');
		$targetColumns = join($this->quoted($this->tc, $table->getColumns()), ',');

		$sqlParameters = [];
		$maxKey = null;

		// set selection range
		if ($keyColumn) {
			$sqlMax = 'SELECT MAX('.$this->tc->quoteIdentifier($keyColumn).') '.
					  'FROM '.$this->tc->quoteIdentifier($table->getName());
			$maxKey = $this->tc->fetchColumn($sqlMax);

			if (isset($maxKey)) {
				$sqlParameters[] = $maxKey;
			}
		} else {
			// clear target table
			$this->truncateTable($this->tc, $table);
		}

		echo $table->getName().': copying ';
		// count selection range
		$sqlCount = 'SELECT COUNT(1) FROM ('.$this->sc->quoteIdentifier($table->getName()).')';
		if ($keyColumn && isset($maxKey)) {
			$sqlCount .= ' WHERE '.$this->sc->quoteIdentifier($keyColumn).' > ?';
		}
		$totalRows = $this->sc->fetchColumn($sqlCount, $sqlParameters);
		echo $totalRows.' rows ('.(($keyColumn) ? 'partial copy' : 'overwrite').")\n";

		$stdout = new StreamOutput($this->output->getStream());
		$progress = new ProgressBar($stdout, $totalRows);
		$progress->setFormatDefinition('debug', ' [%bar%] %percent:3s%% %elapsed:8s%/%estimated:-8s% %current% rows');
		$progress->setFormat('debug');
		$progress->start();

		// transfer sql
		$loopOffsetIndex = 0;

		do {
			// get source data
			$sql = 'SELECT '.$sourceColumns.' FROM '.$this->sc->quoteIdentifier($table->getName());

			// limit selection for PRIMARY KEY mode
			if ($keyColumn) {
				if (isset($maxKey)) {
					$sql .= ' WHERE '.$this->sc->quoteIdentifier($keyColumn).' > ?';
					$sqlParameters = [$maxKey];
				}

				$sql .= ' ORDER BY '.$this->sc->quoteIdentifier($keyColumn).
						' LIMIT '.$this->batch;
			} else {
				$sql .= ' LIMIT '.$this->batch.' OFFSET '.($this->batch * $loopOffsetIndex++);
			}

			if (0 == sizeof($rows = $this->sc->fetchAll($sql, $sqlParameters))) {
				// avoid div by zero in progress->advance
				break;
			}

			$stmt = $this->tc->prepare(
				'INSERT INTO '.$this->tc->quoteIdentifier($table->getName()).' ('.$targetColumns.') '.
				'VALUES ('.join(array_fill(0, count($table->getColumns()), '?'), ',').')'
			);

			$this->tc->beginTransaction();
			foreach ($rows as $row) {
				// remember max key
				if ($keyColumn) {
					$maxKey = $row[$keyColumn];
				}

				$stmt->execute(array_values($row));
				// $progress->advance(1);
			}
			$this->tc->commit();

			$progress->advance(count($rows));

		} while ($keyColumn && sizeof($rows));

		$progress->finish();
		echo "\n\n"; // CRLF after progressbar @ 100%
	}

	protected function execute(InputInterface $input, OutputInterface $output)
	{
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

		if (null == ($tables = $this->getOptionalConfig('tables'))) {
			echo "tables not configured - discovering from source schema\n";
			$tables = [];

			foreach ($this->getTables($sm) as $tableName) {
				echo $tableName." discovered\n";
				$tables[] = [
					'name' => $tableName,
					'mode' => self::MODE_COPY,
				];
			}
		}

		foreach ($tables as $name => $mode) {
			$mode = strtolower($mode);

			// only selected tables?
			if ($selectedTables = $input->getArgument('tables')) {
				if (!in_array($name, array_keys($selectedTables))) {
					continue;
				}
			}

			$table = $sm->listTableDetails($name);

			switch ($mode) {
				case self::MODE_SKIP:
					echo $name.": skipping\n";

					break;
				case self::MODE_COPY:
					$this->copyTable($table, false);

					break;
				case self::MODE_PRIMARY_KEY:
					$keyColumn = $this->getSimplePK($table);
					$this->copyTable($table, $keyColumn);

					break;
				default:
					throw new \Exception('Unknown mode '.$mode);
			}
		}

		if (!$input->getOption('keep-constraints')) {
			$this->addConstraints($tm);
		}
		return 0;
	}
}
