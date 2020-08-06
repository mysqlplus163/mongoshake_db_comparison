# mongoshake_db_comparison
本脚本来自https://github.com/alibaba/MongoShake/,在comparison.py的基础上做了些优化


最近在用shake做迁移mongo集群到集群，源集群不能停，用comparison.py脚本做数据校验，发现脚本一遇到数据不一致就提示然后退出了。
由于线上环境比较复杂，有一个集群拆分到多个集群的（分片总数不一致），有的索引信息和分片不一致的。 源目标集群版本不一致会有些表或多或少。
所以在comparison.py脚本的基础上自己改了下，支持一下功能：
1、支持全量转增量后期的数据校验（能接受某些表数据不一致，会输出源目标count()数对比）
2、新增支持集群单位和库级别分片片校验（config collections）
3、优化日志输出，新增报错日志新增warn级别，对忽略的库和已知的不一致warn提示。新增输出日志颜色区分info，error，warn。看起来更直观。  
