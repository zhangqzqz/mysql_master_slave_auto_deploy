import os

# stop the process when runing
def stop_rep():
    stop_cmd = "ps aux|grep rep_run|grep -v grep | awk '{print $2}'|xargs kill -9"
    reset_cmd = "python rep_run.py -m reset_slave"
    res = os.popen(stop_cmd)
    os.popen(reset_cmd)

    return res

if __name__ == "__main__":
    stop_rep()