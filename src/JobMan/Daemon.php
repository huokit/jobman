<?php
/**
 * @package PHPKit.
 * @author: mawenpei
 * @date: 2016/8/12
 * @time: 8:38
 */
namespace HuoKit\JobMan;

use swoole_process as SwooleProcess;

class Daemon
{
    private $config;
    protected $logger;
    protected $output;
    protected $workers;
    protected $stats;
    protected $pidManager;
    protected $status;
    protected $master_process_name = 'jobman:master';
    protected static $unique_task_list;
    protected $tasks;

    public function __construct($config)
    {
        $this->config = $config;
        $this->pidManager = new PidManager($this->config['pid_path']);
    }

    /**
     * 开启服务
     */
    public function start()
    {
        if($this->pidManager->get()){
            echo 'Error:JobMan is already running' . "\r\n";
            return;
        }
        echo 'JobMan started' . "\r\n";
        if($this->config['daemonize']){
            SwooleProcess::daemon();
        }

        $this->logger = new Logger(['log_path'=>$this->config['log_path']]);
        $this->output = new Logger(['log_path'=>$this->config['output_path']]);
        if(isset($this->config['process_name'])){
            $this->master_process_name = $this->config['process_name'];
        }

        $this->stats = $this->createListenerStats();

        $this->loadCrontabRule();

        swoole_set_process_name($this->master_process_name);

        $this->workers = $this->createWorkers($this->stats);

        $this->registerTimer();

        $this->registerSignal();

        $this->pidManager->save(posix_getpid());

        swoole_timer_tick(1000,function($timerId){
            $statses = $this->stats->getAll();

            foreach($statses as $pid=>$s){
                if ( ($s['last_update'] + $this->config['reserve_timeout'] + $this->config['execute_timeout']) > time()) {
                    continue;
                }
                if (!$s['timeout']) {
                    $this->logger->info("process #{$pid} last upadte at ". date('Y-m-d H:i:s') . ', it is timeout.', $s);
                    $this->stats->timeout($pid);
                }
            }
        });

    }

    /**
     * 停止服务
     */
    public function stop()
    {
        $pid = $this->pidManager->get();
        if (empty($pid)) {
            echo "JobMan is not running...\n";
            return ;
        }

        echo "JobMan is stoping....";
        if(SwooleProcess::kill($pid,0)){
            SwooleProcess::kill($pid,SIGTERM);
        }else{
            $this->pidManager->clear();
        }
        while(1) {
            if ($this->pidManager->get()) {
                sleep(1);
                continue;
            }

            echo "[OK]\n";
            break;
        }
    }

    /**
     * 重启服务
     */
    public function restart()
    {
        $this->stop();
        sleep(1);
        $this->start();
    }

    /**
     * 创建监听器状态表
     * @return ListenerStats
     */
    private function createListenerStats()
    {
        $size = 0;
        foreach($this->config['tubes'] as $tubeName=>$tubeConfig){
            $size += $tubeConfig['worker_num'];
        }
        return new ListenerStats($size,$this->logger);
    }

    /**
     * 加载Crontab执行规则
     */
    private function loadCrontabRule()
    {
        $time = time();

        foreach($this->config['crontabs'] as $name=>$config){
            if(!isset($config['rule'])) continue;
            $ret = ParseCrontab::parse($config['rule'],$time);
            if($ret === false){
                $this->logger->error(ParseCrontab::$error);
            }elseif(!empty($ret)){
                $config['id'] = 0;
                $config['name'] = $name;
                TickTable::set_task($ret,$config);
            }
        }
    }

    private function loadTasks($timer_id)
    {
        $tasks = TickTable::get_task();
        if(empty($tasks)) return false;
        foreach($tasks as $task){
            if(isset($task['unique']) && $task['unique']){
                if (isset(self::$unique_task_list[$task['id']]) && (self::$unique_task_list[$task['id']] >= $task['unique'])) {
                    continue;
                }
                self::$unique_task_list[$task['id']] = isset(self::$unique_task_list[$task['id']]) ? (self::$unique_task_list[$task['id']] + 1) : 0;
            }
            $this->createTaskProcess($task['id'],$task);
        }
    }

    private function createTaskProcess($id,$task)
    {
        $process = new SwooleProcess(function($worker)use($id,$task){
            $worker->name($this->master_process_name . ':' . $task['name'] .':child-crontab-process');
            if(isset($task['class'])){
                $class = $task['class'];
                $man = new $class();
                $man->setLogger($this->logger);
                $man->handle();
            }elseif(isset($task['command'])){
                $worker->exec($task['command']);
                //$this->logger->info($task['command']);
            }

            $worker->exit(1);
        });
        $pid = $process->start();
        $this->tasks[$pid] = $process;
    }

    private function registerTimer()
    {
        swoole_timer_tick(1000,function($timer_id){
            $this->loadTasks($timer_id);
        });
    }

    /**
     * 创建工作进程
     * @param $stats
     * @return array
     */
    private function createWorkers($stats)
    {
        $workers = [];
        foreach ($this->config['tubes'] as $tubeName => $tubeConfig) {
            for($i=0; $i<$tubeConfig['worker_num']; $i++) {
                $worker = new SwooleProcess($this->createTubeLoop($tubeName, $stats), true);
                $worker->start();
                swoole_event_add($worker->pipe, function($pipe) use ($worker) {
                    $recv = $worker->read();
                    $this->output->info($recv);
                    $this->logger->info("recv:" . $recv . " {$pipe} ");
                });

                $workers[$worker->pid] = $worker;
            }
        }

        return $workers;
    }

    /**
     * 任务队列监听器
     * @param $tubeName
     * @param $stats
     * @return \Closure
     */
    private function createTubeLoop($tubeName,$stats)
    {
        return function($process) use ($tubeName, $stats) {

            $process->name($this->master_process_name . ':'.$tubeName.':child-worker-process');
            //创建任务监听器
            $listener = new TubeListener($tubeName, $process, $this->config, $this->logger, $stats);
            $listener->connect();
            $listener->loop();
        };
    }

    /**
     * 注册信号
     */
    private function registerSignal()
    {
        //终止子进程
        SwooleProcess::signal(SIGCHLD,function(){
            while($ret = SwooleProcess::wait(false)){

                if(isset($this->workers[$ret['pid']])){
                    $this->logger->info('process #' . $ret['pid'] . ' exited',$ret);
                    $this->workers[$ret['pid']]->close();
                    unset($this->workers[$ret['pid']]);
                    $this->stats->remove($ret['pid']);
                }

                if(isset($this->tasks[$ret['pid']])){
                    $this->logger->info('process #' . $ret['pid'] . ' exited',$ret);
                    $this->tasks[$ret['pid']]->close();
                    unset($this->tasks[$ret['pid']]);
                }
            }
        });

        //终止进程时等待子进程回收并执行后续清理工作
        $softkill = function($signo){
            if($this->status == 'stoping'){
                return;
            }
            $this->status = 'stoping';
            $this->logger->info('JonMan is stoping....');
            $this->stats->stop();

            swoole_timer_tick(1000,function($timerId){
                if(!empty($this->workers)){
                    return;
                }

                if(!empty($this->tasks)){
                    return;
                }

                //清除定时器
                swoole_timer_clear($timerId);
                //清除主进程pid文件
                $this->pidManager->clear();
                $this->logger->info('JobMan is stopeed');
                exit();
            });
        };
        //终止进程
        SwooleProcess::signal(SIGTERM,$softkill);
        SwooleProcess::signal(SIGINT,$softkill);
    }
}