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