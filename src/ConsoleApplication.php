<?php

namespace DatabaseCopy;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputDefinition;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Application;

/**
 * Base class for console applications
 */
class ConsoleApplication extends Application {

	public function __construct($name = 'UNKNOWN', $version = 'UNKNOWN') {
		parent::__construct($name, $version);

		if (!self::isConsole())
			throw new \Exception('This tool can only be run locally.');
	}

	/**
	 * Check if script is run from console
	 */
	public static function isConsole() {
		return php_sapi_name() == 'cli' || (isset($_SERVER['SESSIONNAME']) && $_SERVER['SESSIONNAME'] == 'Console');
	}

    /**
	 * Returns the long version of the application.
	 *
	 * @return string The long application version
	 */
	public function getLongVersion()
	{
		if ('UNKNOWN' !== $this->getName()) {
			return sprintf('<info>%s</info>', $this->getName());
		}
		return '<info>Console Tool</info>';
	}

	/**
	 * Gets the default input definition.
	 *
	 * @return InputDefinition An InputDefinition instance
	 */
	protected function getDefaultInputDefinition()
	{
		return new InputDefinition(array(
			new InputArgument('command', InputArgument::REQUIRED, 'The command to execute'),
			new InputOption('--help', '-h', InputOption::VALUE_NONE, 'Display this help message.'),
		));
	}
}

?>
