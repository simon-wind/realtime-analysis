# realtime-analysis
实时分析nginx日志，计算pv，uv等指标


数据流为 flume-kafaka-sparkstreaming-http接口传送到open-falcon接口

时延的百分位指标在conf.properties文件里面可以自己配置

    根据split后的日志可以配置请求接口,状态码,设备id,时延五个维度的数据在数组中的位置

    具体指标为：
    
        1：每分钟请求数
        
        2：每分钟错误请求数
        
        3：每分钟各错误码的数量
        
        4：每分钟用户数（精确统计，distinct去重）
        
        5：总用户数（HyperLogLog基数估计）
        
        6: 99th,95th,75,50th的百分位时延
        
    还可以统计各省用户数，异常请求数等，暂时没弄了。
    
    缺点:
    
        百分位的统计参考了breeze的方法，先快排再计算百分位。数据量很大建议采用分治法求第k大的数，做粗略计算（算法导论里面有例子）
        三个job每个job每分钟都会跟falcon的接口建立HTTP连接,链接过于频繁
        
        
