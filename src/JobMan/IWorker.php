<?php
/**
 * @package PHPKit.
 * @author: mawenpei
 * @date: 2016/6/27
 * @time: 17:20
 */
namespace HuoKit\JobMan;

use Psr\Log\LoggerInterface;
use Pheanstalk\Job;

interface IWorker
{
    const FINISH = 'finish';

    const RETRY = 'retry';

    const BURY = 'bury';

    public function execute(Job $data);

    public function setLogger(LoggerInterface $logger);
}