<?php
/**
 * @package PHPKit.
 * @author: mawenpei
 * @date: 2016/8/12
 * @time: 9:05
 */
namespace HuoKit\JobMan\Adapter\Laravel\Command;

use HuoKit\JobMan\Daemon;
use Illuminate\Console\Command;
use Symfony\Component\Console\Input\InputOption;

class JobMan extends Command
{
    protected $name = 'jobman';

    protected $description = 'Get information about Spider';

    protected $daemon;

    public function setDaemon(Daemon $daemon)
    {
        $this->daemon = $daemon;
    }

    public function fire()
    {
        $command = $this->option('command');
        if(!$this->daemon){
            //
        }
        switch($command){
            case 'start':
                $this->daemon->start();
                break;
            case 'stop':
                $this->daemon->stop();
                break;
            case 'restart':
                $this->daemon->restart();
                break;
            case 'reload':
                break;
            default:
                break;
        }
        //$this->line('jobman success');
    }

    public function getOptions()
    {
        return [
            ['command','cmd',InputOption::VALUE_REQUIRED,'',null]
        ];
    }
}