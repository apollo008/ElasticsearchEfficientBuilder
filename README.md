# ElasticsearchEfficientBuilder
    本工具包含了2个高效易用的用以elasticsearch 离线全量建索引和在线实时增量建索引的封装工具：ElasticsearchEfficientFullBuilderFromMysql 和 ElasticsearchEfficientRealtimeBuilderFromKafka 。它们都在elasticsearch 5.x上可以良好的工作运行。
　　注意：从mysql 批量建索引至elasticsearch 原来有个插件叫elasticsearch-jdbc.不过该插件更新很慢，只使用与elasticsearch 2.x版本， 对于本篇的elasticsearch 5.x版本不适用。网上也有json-py-es等开源组件，不过只能从大文件导入索引，都不好用。 

　　本篇实现了java版的builder. 全量建索引的ElasticsearchEfficientFullBuilderFromMysql 和 增量建索引的ElasticsearchEfficientRealtimeBuilderFromKafka，易用高效，亲测可用。相关细节或java api可参考该工程或：https://www.elastic.co/guide/en/elasticsearch/client/java-api/5.4/index.html 。

全量建索引工具ElasticsearchEfficientFullBuilderFromMysql 用法：
![Image text](https://github.com/apollo008/ElasticsearchEfficientBuilder/blob/master/src/main/images/ElasticsearchEfficientFullBuilderFromMysql_help.png)


实时增量建索引工具ElasticsearchEfficientRealtimeBuilderFromKafka用法
![Image text](https://github.com/apollo008/ElasticsearchEfficientBuilder/blob/master/src/main/images/ElasticsearchEfficientRealtimeBuilderFromKafka_help.png)
