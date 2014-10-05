#!/usr/bin/env php
<?php

use DatabaseCopy\Command;
use DatabaseCopy\ConsoleApplication;

function includeIfExists($file) {
	if (file_exists($file)) {
		return include $file;
	}
}

// find autoloader, borrowed from github.com/behat/behat
if ((!$loader = includeIfExists(__DIR__ . '/vendor/autoload.php')) &&
	(!$loader = includeIfExists(__DIR__ . '/../vendor/autoload.php')) &&
	(!$loader = includeIfExists(__DIR__ . '/../../vendor/autoload.php'))) {
	fwrite(STDERR,
		'You must set up the project dependencies, run the following commands:' . PHP_EOL .
		'curl -s http://getcomposer.org/installer | php' . PHP_EOL .
		'php composer.phar install' . PHP_EOL
	);
	exit(1);
}

$app = new ConsoleApplication('Database backup tool');

$app->addCommands(array(
	new Command\BackupCommand,
	new Command\CreateCommand,
	new Command\DropCommand,
	new Command\ClearCommand
));

$app->run();

?>
