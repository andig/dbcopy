<?php

namespace DatabaseCopy\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputDefinition;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class DropCommand extends AbstractCommand {

	protected function configure() {
		$this->setName('drop')
			->setDescription('Drop target schema')
			->addOption('config', 'c', InputOption::VALUE_REQUIRED, 'Config file');
	}

	protected function execute(InputInterface $input, OutputInterface $output) {
		parent::execute($input, $output);

		// make sure schemas exists
		$tm = $this->tc->getSchemaManager();
		$this->validateSchema($this->getConfig('target'));

		if (($db = $this->getOptionalConfig('target.dbname')) !== null) {
			$tm->dropDatabase($db);
		}
		elseif ($this->tc->getDatabasePlatform()->getName() == 'sqlite'
			&& ($db = $this->getOptionalConfig('target.path')) !== null) {
			unlink($db);
		}
		else {
			throw new \Exception('Cannot drop undefined schema');
		}
	}
}

?>
