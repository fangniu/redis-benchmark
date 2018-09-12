# redis-benchmark
类似redis官方的benchmark工具，但是新增很多特性:
1.  支持key类型
2.  支持读写和只写2种压力测试
3.  支持读写比，如7/3表示70%读、30%写
4.  优化测试后的统计信息

具体使用方法可以使用help命令查看

### 例子(ctrl+c结束):
    go run redis-benchmark.go -h 127.0.0.1 -p 6379 -c 100 --rw -t zset --pro 7/3
    2018/09/12 15:47:01 [INFO] redis:  127.0.0.1:6379
    ====== ZADD ======
      149023 requests completed in 16.19 seconds
      0 errors
      3 slow( > 20 milliseconds) 
    99.97% 148984 <= [0-10) milliseconds
    100.00% 36 <= [10-20) milliseconds
    100.00% 2 <= [20-30) milliseconds
    100.00% 1 <= [30-40) milliseconds
    ====== ZREM ======
      149023 requests completed in 16.19 seconds
      0 errors
      4 slow( > 20 milliseconds) 
    99.97% 148979 <= [0-10) milliseconds
    100.00% 40 <= [10-20) milliseconds
    100.00% 3 <= [20-30) milliseconds
    100.00% 1 <= [30-40) milliseconds
    ====== ZINCRBY ======
      149021 requests completed in 16.19 seconds
      0 errors
      3 slow( > 20 milliseconds) 
    99.97% 148970 <= [0-10) milliseconds
    100.00% 48 <= [10-20) milliseconds
    100.00% 2 <= [20-30) milliseconds
    100.00% 1 <= [30-40) milliseconds
    ====== ZRANGE ======
      1043153 requests completed in 16.19 seconds
      0 errors
      31 slow( > 20 milliseconds) 
    99.97% 1042814 <= [0-10) milliseconds
    100.00% 308 <= [10-20) milliseconds
    100.00% 20 <= [20-30) milliseconds
    100.00% 11 <= [30-40) milliseconds
    
    =================
      1490220 requests completed in 16.19 seconds
      0 errors 0.00%
      41 slow 0.00%
      100 parallel clients
      3 bytes payload
      ops: 92028, read[70.00%]: 1043153, write[30.00%]: 447067

指定只测试zset，同时读写比为7:3, 以上基于pc测试的