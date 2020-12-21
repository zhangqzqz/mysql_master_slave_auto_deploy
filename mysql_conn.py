
# -*- coding:utf-8 -*-
import pymysql
import logging
from config_parse import get_config

# logging
logging.basicConfig(
    format="%(levelname)s\t%(asctime)s\t%(message)s", filename="mysql_rep.log")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_all(mysql_args, sql):
    mysql_ip, mysql_user, mysql_port, mysql_pwd = mysql_args
    try:
        conn = pymysql.connect(host=mysql_ip, port=mysql_port,
                           user=mysql_user, passwd=mysql_pwd,  charset='utf8')
        cursor = conn.cursor()
        logger.debug("[%s:%s] mysql db connect: ok", mysql_ip, mysql_user)
        cursor.execute(sql)
        res = list(cursor.fetchall())
        res.sort()
        logger.debug("[%s:%s] sql: %s \nexecute: ok",
                     mysql_ip, mysql_user, sql)
        logger.debug("[%s:%s] result: %s", mysql_ip, mysql_port, res)
        return res
    except Exception as err:
        logger.debug("[%s:%s] sql: %s \nexecute: ok",
                     mysql_ip, mysql_user, sql)
        logger.debug("[%s:%s] result: %s", mysql_ip, mysql_port, err)
        print(f"ERROR:\n{sql}: Failed.\n{err}\n")
        return f"{sql}: Failed.\n {err}\n"
    finally:
        cursor.close()
        conn.close()
        logger.debug("[%s:%s] mysql db close: ok", mysql_ip, mysql_user)

def run_noprint(mysql_args, sql):
    mysql_ip, mysql_user, mysql_port, mysql_pwd = mysql_args
    try:
        conn = pymysql.connect(host=mysql_ip, port=mysql_port,
                           user=mysql_user, passwd=mysql_pwd,  charset='utf8')
        cursor = conn.cursor()
        logger.debug("[%s:%s] mysql db connect: ok", mysql_ip, mysql_user)
        cursor.execute(sql)
        res = list(cursor.fetchall())
        res.sort()
        logger.debug("[%s:%s] sql: %s \nexecute: ok",
                     mysql_ip, mysql_user, sql)
        logger.debug("[%s:%s] result: %s", mysql_ip, mysql_port, res)
        return res
    except Exception as err:
        logger.debug("[%s:%s] sql: %s \nexecute: ok",
                     mysql_ip, mysql_user, sql)
        logger.debug("[%s:%s] result: %s", mysql_ip, mysql_port, err)
        return f"{sql}: Failed.\n {err}\n"
    finally:
        cursor.close()
        conn.close()
        logger.debug("[%s:%s] mysql db close: ok", mysql_ip, mysql_user)


def get_dict(mysql_args, sql):
    mysql_ip, mysql_user, mysql_port, mysql_pwd = mysql_args
    conn = pymysql.connect(host=mysql_ip, port=mysql_port,
                           user=mysql_user, passwd=mysql_pwd,  charset='utf8')
    cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
    logger.debug("[%s:%s] mysql db connect: ok", mysql_ip, mysql_user)

    try:
        cursor.execute(sql)
        res = list(cursor.fetchall())
        logger.debug("[%s:%s] sql: %s \nexecute: ok",
                     mysql_ip, mysql_user, sql)
        logger.debug("[%s:%s] result: %s", mysql_ip, mysql_port, res)
        return res
    except:
        return f"{sql}: Failed.\n"
    finally:
        cursor.close()
        conn.close()
        logger.debug("[%s:%s] mysql db close: ok", mysql_ip, mysql_user)


def more_sql(mysql_args, sqls):
    mysql_ip, mysql_user, mysql_port, mysql_pwd = mysql_args
    try:
        conn = pymysql.connect(host=mysql_ip, port=mysql_port,
                           user=mysql_user, passwd=mysql_pwd,  charset='utf8')
        cursor = conn.cursor()
        logger.debug("[%s:%s] mysql db connect: ok", mysql_ip, mysql_user)
        res = []
        for sql in sqls:
            cursor.execute(sql)
            res.append(list(cursor.fetchall()))
            logger.debug("[%s:%s] sql: %s \nexecute: ok",
                     mysql_ip, mysql_user, sql)
            logger.debug("[%s:%s] result: %s", mysql_ip, mysql_port, list(cursor.fetchall()))
        return res
    except Exception as err:
        logger.debug("[%s:%s] sql: %s \nexecute: ok",
                     mysql_ip, mysql_user, sql)
        logger.debug("[%s:%s] result: %s", mysql_ip, mysql_port, err)
        return f"{sql}: Failed.\n {err}\n"
    finally:
        cursor.close()
        conn.close()
        logger.debug("[%s:%s] mysql db close: ok", mysql_ip, mysql_user)
