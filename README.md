# 需求
    
    不停流动态改变或者增加配置
    
    
  * 步骤：
  * 1,从kafka消费数据
  * 2,在zk中注册配置，在处理数据时动态获取配置
  * 3,然后将处理结果写回kafka

  * 数据：
  * 1,输入数据格式：11,22,33,44,55
  * 2,输出数据为上面的一个字段

  * 目标：
  * 动态改变配置，然后输出不同列的值


# 命令

    #提交命令

    spark2-submit --class streamingKafkaToKafka sparkStreamingDynamicModifyConfig-1.0-SNAPSHOT.jar -brokers datanode2:9092 -groupid 000 -topic in -zkAddress datanode1 -durationTime 10000
    

    #kafka消费命令
    
    kafka-console-consumer --zookeeper datanode1 --topic out
    
    #kafka生产命令
    
    kafka-console-producer --broker-list datanode1:9092 --topic in