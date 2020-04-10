[![Build Status](https://travis-ci.org/flike/SmartPo.svg?branch=master)](https://travis-ci.org/hakwolf/smartProxy-mysql)

# SmartPo简介

SmartPo是一个由Go开发高性能MySQL Proxy项目，SmartPo在满足基本的读写分离的功能上，致力于简化MySQL分库分表操作；能够让DBA通过SmartPo轻松平滑地实现MySQL数据库扩容。 **SmartPo的性能是直连MySQL性能的80%以上**。线上使用SmartPo，**请从[release页面](https://github.com/hakwolf/smartProxy-mysql/releases)获取最新版！！**

## 主要功能：

### 1. 基础功能

- 支持SQL读写分离。
- 支持透明的MySQL连接池，不必每次新建连接。
- 支持平滑上线DB或下线DB，前端应用无感知。
- 支持多个slave，slave之间通过权值进行负载均衡。
- 支持强制读主库。
- 支持主流语言（java,php,python,C/C++,Go)SDK的mysql的prepare特性。
- 支持到后端DB的最大连接数限制。
- 支持SQL日志及慢日志输出。
- 支持SQL黑名单机制。
- 支持客户端IP访问白名单机制，只有白名单中的IP才能访问SmartPo（支持IP 段）。
- 支持字符集设置。
- 支持last_insert_id功能。
- 支持热加载配置文件，动态修改SmartPo配置项（具体参考管理端命令）。
- 支持以Web API调用的方式管理SmartPo。
- 支持多用户模式，不同用户之间的表是权限隔离的，互不感知。

### 2. sharding功能

- 支持按整数的hash和range分表方式。
- 支持按年、月、日维度的时间分表方式。
- 支持跨节点分表，子表可以分布在不同的节点。
- 支持跨节点的count,sum,max和min等聚合函数。
- 支持单个分表的join操作，即支持分表和另一张不分表的join操作。
- 支持跨节点的order by,group by,limit等操作。
- 支持将sql发送到特定的节点执行。
- 支持在单个节点上执行事务，不支持跨多节点的分布式事务。
- 支持非事务方式更新（insert,delete,update,replace）多个node上的子表。

## SmartPo文档

### SmartPo安装和使用

[1.安装SmartPo](./doc/KingDoc/SmartPo_install_document.md)

[2.如何利用一个数据库中间件扩展MySQL集群——SmartPo使用指南](./doc/KingDoc/how_to_use_SmartPo.md)

[3.SmartPo sharding介绍](./doc/KingDoc/SmartPo_sharding_introduce.md)

[4.SmartPo按时间分表功能介绍](./doc/KingDoc/SmartPo_date_sharding.md)

[5.SmartPo 快速入门](./doc/KingDoc/SmartPo_quick_try.md)

[6.管理端命令介绍](./doc/KingDoc/admin_command_introduce.md)

[7.管理端Web API接口介绍](./doc/KingDoc/SmartPo_admin_api.md)

[8.SmartPo SQL黑名单功能介绍](./doc/KingDoc/sql_blacklist_introduce.md)

[9.SmartPo的FAQ](./doc/KingDoc/function_FAQ.md)

[10.SmartPo SQL支持范围](./doc/KingDoc/SmartPo_support_sql.md)

[11.如何配合LVS实现集群部署](./doc/KingDoc/how_to_use_lvs.md)

[12.Kinghshard接入prometheus](./doc/KingDoc/prometheus.md)


### SmartPo架构与设计

[1.SmartPo架构设计和功能实现](./doc/KingDoc/architecture_of_SmartPo_CN.md)

[2.SmartPo性能优化之网络篇](./doc/KingDoc/SmartPo_performance_profiling.md)

[3.SmartPo性能测试报告](./doc/KingDoc/SmartPo_performance_test.md)
## 鸣谢
- 感谢[mixer](https://github.com/siddontang/mixer)作者siddontang, SmartPo最初的版本正是基于mixer开发而来的。
- 感谢[bigpyer](https://github.com/bigpyer)，他对SmartPo做了详细的性能测试，并撰写了一份非常详细的测试报告。
- 感谢以下[开源爱好者](https://github.com/flike/SmartPo/graphs/contributors)为SmartPo做出的贡献。

## SmartPo用户列表

https://github.com/hakwolf/smartProxy-mysql/issues/148

## 反馈
SmartPo开源以来，经过不断地迭代开发，功能较为完善，稳定性有较大提升。 **目前已有超过50家公司在生产环境使用SmartPo作为MySQL代理。** 如果您在使用SmartPo的过程中发现BUG或者有新的功能需求，请发邮件至huager#qq.com与作者取得联系，或者加入QQ群(579720604)交流。
欢迎关注**后端技术快讯**公众号，有关SmartPo的最新消息与后端架构设计类的文章，都会在这个公众号分享。

<img src="./doc/KingDoc/wechat_pic.png" width="20%" height="20%">

## License

SmartPo采用Apache 2.0协议，相关协议请参看[目录](./doc/License)
