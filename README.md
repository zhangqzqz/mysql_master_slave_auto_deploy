<<<<<<< HEAD
# MySQL_Master_Slave_auto_deploy

mysql主从部署脚本

修订记录

| 作者 | 版本 | 时间       | 备注       |
| ---- | ---- | ---------- | ---------- |
| 张全针 | v1.0 | 2019/11/1 |           |
| 张全针 | v1.1 | 2019/11/5 |            |
| 张全针 | v1.1.1 | 2019/11/18 |            |
| 张全针 | v1.2 | 2019/12/03 |            |
| 张全针 | v1.2.1 | 2019/12/16 |  支持rds          |
| 张全针 | v1.3 | 2020/01/02 |  支持主从切换        |
| 张全针 | v1.4 | 2020/01/17 |  支持innobackup物理初始化       |
| 张全针 | v2.0 | 2020/03/06 |  支持oms工具箱      |

## 定位
        给MC公司内部人员使用，提高自动化mysql主从部署的效率。


## 自动化部署工具支持运行的软件

        与目标环境网络通畅的环境即可

## 支持的操作系统和数据库版本配对
        操作系统支持：rehl5，rehl6，rehl7
        数据库版本支持：mysql5.5（不支持GTID模式）,mysql5.6,mysql5.7,mysql8.0




## 使用说明


### 主从库Linux运行环境

在主库及从库linux环境的/etc/sudoers文件添加以下一行，以开启<MYSQL>用户sudo权限：
vi /etc/sudoers

如：

mysql   ALL=(ALL)       NOPASSWD: ALL


### python运行环境

python 3


        1. 进入脚本目录
        pip install -r requirements.txt    #安装项目python依赖包pymysql,prettytable与paramiko。  #第一次安装时用
    
        如果网速慢，可以用以下命令安装，使用国内的PIP源

   ```
   pip install -i http://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com -r requirements.txt
   ```

### mysql运行环境
        主备库用于部署数据主从用户需赋权，如下
        1.mysql5.5 5.6 5.7 用以下语句赋权
        GRANT ALL PRIVILEGES ON *.* TO '用户名'@'%' IDENTIFIED BY '用户密码' WITH GRANT OPTION;
        2.mysql8.0需使用以下语句赋权，并使用mysql_native_password身份验证模式
        create user '用户名'@'%' IDENTIFIED BY '用户密码';
        grant all privileges on *.* to '用户名'@'%' WITH GRANT OPTION;
        ALTER USER '用户名'@'%' IDENTIFIED WITH mysql_native_password BY '用户密码';
    
        注意：部署主从对数据库还有以下要求：
        1.主从库的server id 需要设置成不同的值
        2.主从库需要开启log bin
        3.主从库大版本需要一致，例如都为mysql5.5

### 配置文件
配置文件名为：config.cfg，配置示例如下：
        
        [master_config]                             --该参数项项目名必备
        ip = 192.168.239.51
        os_user = root
        os_password = hzmcdba
        os_port = 22
        db_user = root                              --上面赋权的用户名       
        db_port = 3001
        db_password = mysql
        db_config_dir = /mysql_data_57/my.cnf
        xbackup_dir = /backup/bak                   --数据库初始化备份路径，主库必备参数,需要使用二级目录
        
        [slave_config]                              --从库参数项参数名必须包含'slave_config'
        ip = 192.168.239.52
        os_user = root
        os_password = hzmcdba
        os_port = 22
        db_user = root                              --上面赋权的用户名 
        db_port = 3001
        db_password = mysql
        db_config_dir = /mysql_data_57/my.cnf


​        
        [slave_config_a]
        ip = 192.168.239.52
        os_user = root
        os_password = hzmcdba
        os_port = 22
        db_user = root                               --上面赋权的用户名 
        db_port = 3005
        db_password = mysql
        db_config_dir = /mysql_data_57_2/my.cnf


## 代码说明

| 文件或目录       | 功能                 | 备注                                                         |
| --------------- | -------------------- | ------------------------------------------------------------ |
| rep_run.py      | 命令行主入口         |                                                             |
| method.py       | 主要功能和函数方法    |                                                              |
| config_parse.py | 配置文件解析函数      |                                                              |
| mysql_rep.log   | 安装脚本输出结果日志  |                                                              |
| mysql_rep.py    | 主从部署函数          |                                                             |
| mysql_switch.py    | 主从切换函数          |                                                             |
| stop_rep.py    | 停止配置并reset函数          |     python stop_rep.py                                                        |

## 主程序的使用
python rep_run.py  参数

可以使用python rep_run.py -h 查看脚本使用帮助。

举例：

半同步复制下的传统模式配置，则执行：

cd <解压路径>

python rep_run.py -m semi_normal



### 命令行参数介绍

  -c CONF, --conf CONF  配置文件名，默认为本目录下的'config.cfg'
  -t INIT_DB_METHOD, --init_db_method INIT_DB_METHOD 从库数据初始化方式， 可选：|mysqldump：使用mysqldump初始化从库 |innobackup: 物理备份初始化从库,默认为空
  -m MODE, --mode MODE 选择配置部署模式或其他功能,有以下选择：
        rep_status：查看主从状态
        reset_slave：重置主从配置
        semi_gtid：半同步复制下的GTID模式配置
        semi_gtid_rds：rds的半同步复制下的GTID模式配置 （请在该模式配置前使用mysqldump初始化数据库）
        semi_normal：半同步复制下的传统模式配置
        async_gtid：异步复制下的GTID模式配置
        async_normal：异步复制下的传统模式配置
        check_switch: 检查主从同步状态
        check_pos: 检查主从同步position状态
        mysql_switch: 执行主从切换,切换过程中，可指定待切换成主库的备库
        innobackup: 物理初始化从库

### 日志查看

    相关日志可查看解压路径下的mysql_rep.log


