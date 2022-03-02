# LSM-tree
本项目是基于LSM Tree的键值对存储系统，支持PUT、GET、DEL操作，其中K为64位有符号整数，V为字符串，其中memtable使用跳表实现，能够实现意外终止时的持久化和恢复。