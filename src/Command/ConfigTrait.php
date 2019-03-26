<?php

namespace DatabaseCopy\Command;

use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Yaml\Yaml;

trait ConfigTrait
{
	protected $config;	// configuration

	protected function getConfig($keys = null, $required = true, $config = null) {
		// use $this->config if context not specified
		if ($config == null)
			$config = $this->config;

		if ($keys !== null) {
			$path = explode('.', $keys);
			foreach ($path as $key) {
				if (isset($config[$key])) {
					$config = $config[$key];
				}
				else {
					$config = null;
					break;
				}
			}
		}

		if ($required && $config == null)
			throw new \Exception('Configuration error: ' . $keys . ' is undefined at level ' . $key);

		return $config;
	}

	protected function getOptionalConfig($keys = null, $config = null) {
		return $this->getConfig($keys, false, $config);
	}

	protected function loadConfig(InputInterface $input) {
		$CONFIG_FILE = 'config.yaml';

        if (($file = $input->getOption('config')) == null) {
            // current folder
            $file = getcwd() . DIRECTORY_SEPARATOR . $CONFIG_FILE;

			if (!file_exists($file)) {
                // program folder
                if (file_exists($alt = realpath(__DIR__) . DIRECTORY_SEPARATOR . $CONFIG_FILE)) {
					$file = $alt;
				}
            }
        }

		$this->config = Yaml::parseFile($file);
	}
}
