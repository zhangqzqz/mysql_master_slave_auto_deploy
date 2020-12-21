#!/usr/bin/env python
import paramiko
import logging

# logging
logging.basicConfig(format="%(levelname)s\t%(asctime)s\t%(message)s",filename="mysql_rep.log")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)



def ssh_input(args, cmd):
    
    host, port, username, password = args
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, port, username, password, look_for_keys=False)
    logger.debug("[%s:%s] connect: ok", host, port)

    try:
        remote_command = cmd
        stdin, stdout, stderr = client.exec_command(remote_command)
        logger.debug("[%s:%s %s] cmd: %s \nexecute: ok", host, port,username, remote_command)

        result = stdout.readlines()
        err = stderr.readlines()
        
        logger.debug("[%s:%s] result: %s", host, port, "".join(result))
        if err!=[] and "stty: standard " not in err[0] and "[Warning]" not in err[0] \
            and 'Using a password on the command line interface can be insecure' not in err[0]:
            logger.debug("[%s:%s] error: %s", host, port, "".join(err))
            print("".join(err))
            return err
        if '[YOU HAVE NEW MAIL]\n' in result:
            result.remove('[YOU HAVE NEW MAIL]\n')
        result = [ i for i in result if 'Last login ' not in i and i != '\n'
         and 'Using a password on the command line interface can be insecure' not in i]
        return result
    finally:
        client.close()

        logger.debug("[%s:%s] close: ok", host, port)


def ssh_input_noprint(args, cmd):
    
    host, port, username, password = args
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, port, username, password, look_for_keys=False)
    logger.debug("[%s:%s] connect: ok", host, port)

    try:
        remote_command = cmd
        stdin, stdout, stderr = client.exec_command(remote_command)
        logger.debug("[%s:%s %s] cmd: %s \nexecute: ok", host, port,username, remote_command)

        result = stdout.readlines()
        err = stderr.readlines()
        
        logger.debug("[%s:%s] result: %s", host, port, "".join(result))
        if err!=[] and "stty: standard " not in err[0] and 'Using a password on the command line interface can be insecure' not in err[0]:
            logger.debug("[%s:%s] error: %s", host, port, "".join(err))
            return err
        if '[YOU HAVE NEW MAIL]\n' in result:
            result.remove('[YOU HAVE NEW MAIL]\n')
        result = [ i for i in result if 'Last login ' not in i and i != '\n'
         and 'Using a password on the command line interface can be insecure' not in i]
        return result
    finally:
        client.close()

        logger.debug("[%s:%s] close: ok", host, port)

def ssh_ftp(args,remotepath,localpath,mode):
    host, port, username, password = args

    tran = paramiko.Transport((host, port))
    tran.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(tran)
    logger.debug("[%s:%s] connect: ok", host, port)
    try:
        if mode=='put':
            sftp.put(localpath,remotepath)
            logger.debug("[%s:%s] put '%s' to '%s'",host, port, localpath,remotepath)
        elif mode=='get':
            sftp.get(remotepath, localpath)
            logger.debug("[%s:%s] get '%s' from '%s'",host, port, remotepath, localpath)
    finally:
        tran.close()
        logger.debug("[%s:%s] close: ok", host, port)

