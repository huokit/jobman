<?php
/**
 * @package PHPKit.
 * @author: mawenpei
 * @date: 2016/6/27
 * @time: 18:45
 */
namespace HuoKit\JobMan;

class TubeListener
{
    protected $tubeName;

    protected $process;

    protected $queue;

    protected $logger;

    protected $stats;

    protected $times = 0;

    public function __construct($tubeName, $process, $config, $logger, $stats)
    {
        $this->tubeName = $tubeName;
        $this->process = $process;
        $this->config = $config;
        $this->logger = $logger;
        $this->stats = $stats;
    }

    public function connect()
    {
        $tubeName = $this->tubeName;
        $process = $this->process;
        $logger = $this->logger;
        try {
            $this->queue = new BeanstalkClient($this->config['host'], $this->config['port']);
        }catch(\Exception $e){
            $logger->error('queue server host:'.$this->config['host'].' port:' . $this->config['port'] . ' connect fail');
            return false;
        }
        //$this->logger->info('connect queue success');
        $this->queue->watch($tubeName);
        $logger->info("tube({$tubeName}, #{$process->pid}): watching.");

        return true;
    }

    public function loop()
    {
        $tubeName = $this->tubeName;
        $logger = $this->logger;
        $process = $this->process;
        $worker = $this->createQueueWorker($tubeName);

        while(true) {
            $this->stats->touch($tubeName, $process->pid, false, 0);
            $stoping = $this->stats->isStoping();
            if ($stoping) {
                $this->logger->info("process #{$process->pid} is exiting.");
                $process->exit(1);
                break;
            }

            $job = $this->reserveJob();

            if ($job===null) {
                $this->logger->info('job is not exists');
                sleep(1);
                continue;
            }

            try {
                $result = $worker->execute($job);
                $this->stats->touch($tubeName, $process->pid, false, 0);
            } catch(\Exception $e) {
                $message = sprintf('tube({$tubeName}, #%d): execute job #%d exception, `%s`', $process->pid, $job->getId(), $e->getMessage());
                $logger->error($message, ['data'=>$job->getData()]);
                continue;
            }

            $code = is_array($result) ? $result['code'] : $result;

            switch ($code) {
                case IWorker::FINISH:
                    $this->finishJob($job, $result);
                    break;
                case IWorker::RETRY:
                    $this->retryJob($job, $result);
                    break;
                case IWorker::BURY:
                    $this->buryJob($job, $result);
                    break;
                default:
                    break;
            }

        }


    }

    private function reserveJob()
    {
        $tubeName = $this->tubeName;
        $logger = $this->logger;
        $process = $this->process;

        if ($this->times % 10 === 0) {
            $logger->info("tube({$tubeName}, #{$process->pid}): reserving {$this->times} times.");
        }

        //$this->logger->info('watch queue reserve:' . $tubeName . ' timeout:' . $this->config['reserve_timeout']);

        $job = $this->queue->reserve($this->config['reserve_timeout']);
        //$this->logger->info(json_encode($job));
        $this->times ++;
        $jobId = $job !== false ? $job->getId() : 0;
        $this->stats->touch($tubeName, $process->pid, true,$jobId);

        if ($job===false) {
            //$this->logger->info('job is not exists');
            //$this->process->exit(1);
            return null;
        }

        $logger->info("tube({$tubeName}, #{$process->pid}): job #{$job->getId()} reserved.", ['data'=>$job->getData()]);

        return $job;
    }

    private function finishJob($job, $result)
    {
        $tubeName = $this->tubeName;
        $queue = $this->queue;
        $logger = $this->logger;
        $process = $this->process;

        $logger->info("tube({$tubeName}, #{$process->pid}): job #{$job->getId()} execute finished.");

        $queue->delete($job);

    }

    private function retryJob($job, $result)
    {
        $tubeName = $this->tubeName;
        $queue = $this->queue;
        $logger = $this->logger;
        $process = $this->process;

        $message = json_decode($job->getData(),true);
        if (!isset($message['retry'])) {
            $message['retry'] = 0;
        } else {
            $message['retry'] = $message['retry'] + 1;
        }
        $stats = $queue->statsJob($job);
        if ($stats === false) {
            $logger->error("tube({$tubeName}, #{$process->pid}): job #{$job->getId()} get stats failed, in retry executed.", ['data'=>$job->getData()]);
            return;
        }

        $logger->info("tube({$tubeName}, #{$process->pid}): job #{$job->getId()} retry {$message['retry']} times.");
        $deleted = $queue->delete($job);
        if (!$deleted) {
            $logger->error("tube({$tubeName}, #{$process->pid}): job #{$job->getId()} delete failed, in retry executed.", ['data'=>$job->getData()]);
            return;
        }

        $pri = isset($result['pri']) ? $result['pri'] : $stats['pri'];
        $delay = isset($result['delay']) ? $result['delay'] : $stats['delay'];
        $ttr = isset($result['ttr']) ? $result['ttr'] : $stats['ttr'];

        $puted = $queue->put($tubeName,json_encode($message),$pri, $delay, $ttr);
        if (!$puted) {
            $logger->error("tube({$tubeName}, #{$process->pid}): job #{$job->getId()} reput failed, in retry executed.", ['data'=>$job->getData()]);
            return;
        }

        $logger->info("tube({$tubeName}, #{$process->pid}): job #{$job->getId()} reputed, new job id is #{$puted}");

    }

    private function buryJob($job, $result)
    {
        $tubeName = $this->tubeName;
        $queue = $this->queue;
        $logger = $this->logger;
        $process = $this->process;

        $stats = $queue->statsJob($job);
        if ($stats === false) {
            $logger->error("tube({$tubeName}, #{$process->pid}): job #{$job->getId()} get stats failed, in bury executed.", ['data'=>$job->getData()]);
            return;
        }

        $pri = isset($result['pri']) ? $result['pri'] : $stats['pri'];
        $burried = $queue->bury($job, $pri);
        if ($burried === false) {
            $logger->error("tube({$tubeName}, #{$process->pid}): job #{$job->getId()} bury failed", ['data'=>$job->getData()]);
            return;
        }

        $logger->info("tube({$tubeName}, #{$process->pid}): job #{$job->getId()} buried.");

    }

    private function createQueueWorker($name)
    {
        $class = $this->config['tubes'][$name]['class'];
        //$this->logger->info('worker class file:' . $class);
        $worker = new $class($name, $this->config['tubes'][$name]);
        $worker->setLogger($this->logger);
        //$this->logger->info('worker class instance success');
        return $worker;
    }

    public function getQueue()
    {
        return $this->queue;
    }
}