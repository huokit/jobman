<?php
/**
 * @package PHPKit.
 * @author: mawenpei
 * @date: 2016/7/28
 * @time: 19:25
 */
namespace HuoKit\JobMan;

use Psr\Log\LoggerInterface;

interface ITask
{

    public function handle();
    public function setLogger(LoggerInterface $logger);
}