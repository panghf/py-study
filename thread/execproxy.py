# encoding: utf-8
import codecs
import time
import  threading as thread

flag=0 #标识是否本次运行第1次打开log文件
ISOTIMEFORMAT ='%Y-%m-%d %X'
logFile=file('execproxy.log','a+')

loglock=thread.Lock() #获得一个线程锁用于写log文件
tllock=thread.Lock() #获得一个线程锁用于taskList
phllock=thread.Lock() #获得一个线程锁用于procHostList

def log(msg):
    '''
    写日志到文件里，避免控制台输出搞乱了任务输入
    :param msg: 要输出的日志
    :return:
    '''
    global  flag
    global ISOTIMEFORMAT
    global mylock
    loglock.acquire()  #获得线程锁loglock
    if flag==0:
        logFile.write('=============================\n')
        flag=1
    ts=time.strftime(ISOTIMEFORMAT, time.localtime())
    msg=ts+' '+msg
    logFile.writelines(msg+'\n')
    logFile.flush()
    loglock.release()


class Task:
    '''
    任务对象定义
    '''
    def __init__(self,taskId,hostIp,sleepTime):
        self.taskId=taskId #任务ID
        self.hostIp=hostIp #主机ID
        self.sleepTime=sleepTime#睡眠多少时间

    def __str__(self):
        return '任务(ID={0},Ip={1})'.format(self.taskId, self.hostIp)

    def __repr__(self):
        return '任务(ID={0},Ip={1})'.format(self.taskId,self.hostIp)


class ReadTask(thread.Thread):
    '''
    模拟从Server端读取任务的线程
    '''
    def __init__(self,taskList):
        thread.Thread.__init__(self)  # 默认初始化
        self.taskList=taskList

    def run(self):
        count=0
        while True:
            try:
                s=raw_input('请输入任务信息：')
                if s=='quit':
                    log ('任务读取结束。')
                    break
                elif s=='mock':
                    log('mock任务......')
                    s='task,192.168.0.1,10' #模拟192.168.0.1主机上执行任务，10秒钟
                ts=s.split(',')
                count = count + 1
                t=Task(ts[0]+str(count),ts[1],float(ts[2]))
                tllock.acquire()  #获得线程锁tllock
                log('收到任务:'+str(t))
                self.taskList.append(t)
                tllock.release() #释放锁
            except Exception as ex:
                log ('输入格式错误，正确格式：taskId,hostIp,sleepTime')

class ExecProxy(thread.Thread):
    '''
    模拟我们的执行网关（ExecProxy），从任务队列里读取任务，
    然后判断是否能执行（如果没有主机在执行则可以），可以的话交由执行线程ProcessTask处理
    '''
    def __init__(self, taskList, procHostList):
        thread.Thread.__init__(self)  # 默认初始化
        # 任务队列
        self.taskList = taskList
        # 正在处理的主机列表
        self.procHostList = procHostList


    def run(self):
        log ('开始ExecProxy执行...')
        t = Task('None','0.0.0.0',1)
        while True:
            # 这里采用简单的2秒轮询一次的办法读取队列里的所有任务进行处理
            # 更好的办法是用MQ，或者用线程通知（Observer）模式实现立即处理
            #如果用MQ就不需要用线程锁了
            tllock.acquire()  # 获得线程锁tllock
            if len(taskList)>0:
                log("任务队列："+str(taskList))
            for t in taskList:
                # 判断t是否可以处理
                host = t.hostIp
                taskId=t.taskId
                #这里改成判断任务是否在删除队列里，如果在则丢弃该任务
                #我这里模拟丢弃task5
                if taskId=='task5':
                    taskList.remove(t)  # 从队列里删除任务
                    log('任务取消执行：'+str(t))
                    continue

                #这里也要判断phlLock是否可用
                phllock.acquire()  #获得线程锁phllock
                if host in procHostList:        # 如果主机已经在执行任务，则先不处理
                    log('主机'+host+'有任务在执行，无法执行任务：'+str(t))
                    log('执行主机列表:'+str(procHostList))
                    phllock.release()  # 释放锁
                    continue

                log('提交任务' + str(t))
                # 将任务写入处理列表
                procHostList.append(t.hostIp)
                p=ProcessTask(t,procHostList)
                p.start()

                phllock.release()  # 释放锁

                taskList.remove(t)  #从队列里删除任务

            tllock.release()  # 释放锁
            # 休眠2秒
            thread._sleep(2)


class ProcessTask(thread.Thread):
    '''
    模拟SaltMaster执行任务，执行就是sleep任务里的sleepTime时长
    这里简单的一个线程处理一个任务，开发处理时将IP加入处理队列，处理完后删除
    '''
    def __init__(self,task,procHostList):
        thread.Thread.__init__(self)  # 默认初始化
        #要处理的任务
        self.task=task
        #正在处理的主机列表
        self.procHostList=procHostList

    def run(self):
        log('开始处理任务：' + str(self.task))
        thread._sleep(self.task.sleepTime)
        log ('完成任务处理：' + str(self.task))
        phllock.acquire()  # 获得线程锁phllock
        self.procHostList.remove(self.task.hostIp)
        phllock.release()  # 释放锁

if __name__ == "__main__":
    log('program start......')
    #任务存储队列
    taskList=[]
    #执行中的主机列表
    procHostList=[]

    #读取任务线程启动
    rt=ReadTask(taskList)
    rt.start()

    #执行代理线程启动
    ep=ExecProxy(taskList,procHostList)
    ep.start()

