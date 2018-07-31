# mysql binlog listener

### you can use 
    redis-cli -hxxx -pxxx
        >> get schema.table.field_name to get a sql that convert from dumped binlog
        

### TODO:
- redis proto
- decode for all field type
- decode & rollback update event
- when mysql meta data change, should sync with dumper.meta
- format event binlog 开始, rotate event 结束

- 启动时, 从 json 文件中恢复 binlog, 并得到继续同步的 binlog 位置
    - 解析成功的 json, 表示对应的 binlog 文件 以 stop/rotate 结尾, 完整接收到了 binlog
    - 解析json失败， 则表示当前 json 对应的 binlog 没有完整的接收到, 应删除当前文件, 并继续从当前 binlog:4 开始同步