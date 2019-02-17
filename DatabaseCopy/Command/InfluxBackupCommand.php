<?php

namespace DatabaseCopy\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Output\StreamOutput;
use Symfony\Component\Console\Helper\ProgressBar;

use InfluxDB\Client as InfluxClient;
use InfluxDB\Database as InfluxDatabase;
use InfluxDB\Point as InfluxPoint;
use Doctrine\DBAL\DriverManager as DoctrineDriverManager;


class InfluxBackupCommand extends AbstractCommand {

	const PREC = InfluxDatabase::PRECISION_MILLISECONDS;
	const INFLUX_READ_OPTIONS = ['epoch' => self::PREC];
	const INFLUX_ATTRIBUTES = ['uuid' => 1, 'type' => 1, 'title' => 1];
	const BATCHSIZE = 4 * 1024;

	protected $batch;			// batch size
	protected $influxdb;

	protected function configure() {
		$this->setName('influx')
			->setDescription('Copy data to InfluxDB')
			->addOption('config', 'c', InputOption::VALUE_REQUIRED, 'Config file')
			->addOption('mode', 'm', InputOption::VALUE_REQUIRED, 'Mode (full|delta)', 'delta')
			->addOption('delete', 'd', InputOption::VALUE_NONE, 'Delete series from InfluxDB')
			->addOption('filter', 'f', InputOption::VALUE_REQUIRED, 'Filter by channel name (default no filter). Seperate multiple by comma.', '')
			->addOption('batch', 'b', InputOption::VALUE_REQUIRED, 'Batch size (default 32k)', self::BATCHSIZE);
	}

	protected function influxConnect() {
		$dsn = $this->getConfig('influx.dsn');

		if (stripos($dsn, "influxdb://") === false) {
			$dsn = 'influxdb://' . $dsn;
		}

		$client = InfluxClient::fromDSN($dsn);
		$this->influxdb = $client->selectDB($this->getConfig('influx.dbname'));
	}

	protected function influxQuery(string $sql, array $options = []): array {
		$options = array_merge(self::INFLUX_READ_OPTIONS, $options); // apply default options
		$stmt = $this->influxdb->query($sql, $options);
    	return $stmt->getPoints();
	}

	protected function influxDelete(array $entity) {
		$sql = sprintf(
			'DELETE FROM %s WHERE "title"=\'%s\'',
			$this->getConfig('influx.measurement'),
			$entity['title']
		);
		$this->influxdb->query($sql);
	}

	protected function countRemainingRows(array $entity, int $timestamp): int {
		$countSql = <<<EOD
SELECT
    count(1) AS count
FROM data d
WHERE
    d.channel_id = ?
    AND d.timestamp >= ?
EOD;

		$res = $this->sc->executeQuery($countSql, [$entity['id'], $timestamp])->fetchAll();
        return $res[0]['count'];
	}

	protected function fetchEntities(array $filters): array {
		$entitiesSql = <<<EOD
SELECT
    e.id, e.uuid, e.type, p.value AS title
FROM entities e
INNER JOIN properties p ON
    e.id = p.entity_id
WHERE
    e.class = "channel"
    AND p.pkey = "title"
EOD;

		if (count($filters)) {
			$names = array_map(function($name) {
				return sprintf('p.value = "%s"', $name);
			}, $filters);

			$entitiesSql .= sprintf(' AND (%s)', join(' OR ', $names));
		}

		return $this->sc->executeQuery($entitiesSql)->fetchAll();
	}

	protected function influxLastTimestamp(array $entity): int {
		$iql = <<<EOD
SELECT last(value)
FROM data
WHERE uuid = '%s'
EOD;

		$res = $this->influxQuery(sprintf($iql, $entity['uuid']));
		$timestamp = count($res) ? $res[0]['time'] : 0;

		return $timestamp;
	}

	protected function copyEntity(array $entity, int $timestamp, callable $callback = null) {
		$measurement = $this->getConfig('influx.measurement');

		$sql = <<<EOD
SELECT
    d.timestamp, d.value
FROM data d
WHERE
    d.channel_id = ?
    AND d.timestamp >= ?
LIMIT %d
EOD;

        $dataSql = sprintf($sql, $this->batch); // block size

		do {
			$stmt = $this->sc->executeQuery($dataSql, [$entity['id'], $timestamp]);
			$res = $stmt->fetchAll();

			$points = [];
			foreach ($res as $row) {
				$timestamp = (int)$row['timestamp']; // update last timestamp

				$points[] = new InfluxPoint(
					$measurement,
					(float)$row['value'],
					array_intersect_key($entity, self::INFLUX_ATTRIBUTES),
					[], // optional
					$timestamp
				);
			}

			$this->influxdb->writePoints($points, self::PREC);

			if ($callback) {
				$callback(count($res));
			}
		} while (count($res) == $this->batch);
	}

	protected function execute(InputInterface $input, OutputInterface $output) {
        $this->readConfig($input);

		// Doctrine PDO
        $this->sc = DoctrineDriverManager::getConnection($this->getConfig('source'));
		// make sure all connections are UTF8
        if ($this->sc->getDatabasePlatform()->getName() == 'mysql') {
            $this->sc->executeQuery("SET NAMES utf8");
        }

		// Influx
		$this->influxConnect();

		$this->batch = $input->getOption('batch');
        $filters = preg_split('/,/', $input->getOption('filter'), null, PREG_SPLIT_NO_EMPTY);
		$mode = $input->getOption('mode');

		$entities = $this->fetchEntities($filters);

		foreach ($entities as $idx => $entity) {
			printf("\n\n%d/%d %s\n", $idx+1, count($entities), $entity['title']);

			if ($input->getOption('delete')) {
				$this->influxDelete($entity);
				die("delete");
			}
			$timestamp = $mode == 'delta' ? $this->influxLastTimestamp($entity) : 0;

			$count = $this->countRemainingRows($entity, $timestamp);

			// progress bar
			$stdout = new StreamOutput($output->getStream());
			$progress = new ProgressBar($stdout, $count);
			$progress->setFormatDefinition('debug', ' [%bar%] %percent:3s%% %elapsed:8s%/%estimated:-8s% %current% rows');
			$progress->setFormat('debug');

			$progress->start();
            $this->copyEntity($entity, $timestamp, function ($count) use ($progress) {
                $progress->advance($count);
            });
            $progress->finish();
		}

	}
}
