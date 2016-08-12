<?php
/**
 * @package PHPKit.
 * @author: mawenpei
 * @date: 2016/7/4
 * @time: 17:06
 */
namespace HuoKit\JobMan\Adapter\Laravel;

use Illuminate\Support\Facades\Config;

class JobManager
{
    protected static $client;

    public static function getClient()
    {
        if(!isset(self::$client)){
            self::$client = new BeanstalkClient(Config::get('jobman.host'),Config::get('jobman.port'));
        }
        return self::$client;
    }

    /**
     *
     * @param $tubeName
     * @param $data
     * @param int $priority
     * @param int $delay
     * @param int $ttr
     * @return int
     */
    public static function put($tubeName,$data,$priority=1024,$delay=0,$ttr=60)
    {
        $data = json_encode($data);
        return self::getClient()->putInTube($tubeName,$data,$priority,$delay,$ttr);
    }
}