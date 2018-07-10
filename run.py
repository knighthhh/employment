from time import sleep

from scheduler import Scheduler
from db import MongoClient, MysqlClient,RedisClient
import multiprocessing
import threading

def main():
    s = Scheduler()
    print('程序开始运行。。')
    redisClient = RedisClient()
    flag = True
    while flag:
        redis_len = redisClient.llen('employment')
        print('redis队列长度：' + str(redis_len))
        if redis_len > 0:
            s.run()
        else:
            flag = False

if __name__ == '__main__':
    for i in range(8):
        p = multiprocessing.Process(target=main)
        p.start()

