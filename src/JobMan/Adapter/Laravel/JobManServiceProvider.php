<?php
/**
 * @package PHPKit.
 * @author: mawenpei
 * @date: 2016/6/27
 * @time: 17:18
 */
namespace HuoKit\JobMan\Adapter\Laravel;

use HuoService\JobMan\Command\JobMan;
use Illuminate\Support\Facades\Config;
use Illuminate\Support\ServiceProvider;

class JobManServiceProvider extends ServiceProvider
{
    public function boot()
    {
    }

    protected function registerCommands()
    {
        $this->app->bindIf('command.jobman', function () {
            $jobman = new JobMan();
            $daemon = new Daemon(Config::get('jobman'));
            $jobman->setDaemon($daemon);
            return $jobman;
        });

        $this->commands([
            'command.jobman'
        ]);
    }

    public function register()
    {
        $this->registerCommands();
    }

    public function provides()
    {
        return [
            'command.jobman'
        ];
    }
}