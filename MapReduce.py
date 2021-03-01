from abc import ABC, abstractmethod
import os,re,zmq,time
from multiprocessing import Process, Array, Value



class MapReduce:
    NumWorkers = 0
    def __init__(self,NumWorkers):
        self.NumWorkers = NumWorkers
    @abstractmethod
    def Map(self,parts):
        pass
    @abstractmethod
    def Reduce(self, kvs):
        pass
    # @abstractmethod
    def start(self,filename):
        # pid = os.getpid()
        # print('Producer: ', pid)
        with open(filename, 'r', encoding='utf-8') as file:
            text = file.readlines()
        print(text)

        Producer = Process(target=self.Producer, args=(text,))
        Producer.start()
        # Producer.join()

        consumers = []
        for i in range(0, self.NumWorkers):
            p = Process(target=self.Consumer, args=(i,))
            p.start()
            consumers.append(p)


        collector = Process(target =self.ResultCollector)
        collector.start()

        Producer.join()
        collector.join()

        for p in consumers:
            p.terminate()
            p.join()

        # self.Producer()
        # self.Consumer()
        # self.ResultCollector()
    # @abstractmethod
    def Producer(self,textlist):
        pid = os.getpid()
        print('Producer: ', pid)
        # with open(filename, 'r', encoding='utf-8') as file:
        #     text = file.readlines()

        print('Producer: ', len(textlist))
        context = zmq.Context()
        push_socket = context.socket(zmq.PUSH)
        push_socket.bind("tcp://127.0.0.1:8000")
        chunks = []
        remaining = len(textlist) % self.NumWorkers
        if remaining > 0:
            total = (len(textlist)-remaining) // self.NumWorkers
            chunks = [textlist[x:x+total] for x in range(0, len(textlist)-remaining, total)]
            for times in range(1,remaining+1):
                chunks[times-1].append(textlist[-times])
            # print(total)
        else:
            total = len(textlist) // self.NumWorkers
            chunks = [textlist[x:x+total] for x in range(0, len(textlist), total)]
        
        print(chunks)
        
        for i in range(len(chunks)):
            push_socket.send_json([chunks[i]])
            time.sleep(0.25)
        

        
    def Consumer(self,id_worker):
        pid = os.getpid()
        print('Consumer: ', pid)

        context = zmq.Context()
        pull_socket = context.socket(zmq.PULL)
        pull_socket.connect("tcp://127.0.0.1:8000")
        mapped = dict()
        message = []
        while True:
            message = pull_socket.recv_json()
            if len(message) > 0:
                print("PID Consumer",pid,message)
                token_list = []
                for mes in message:
                    for i in mes:
                        # liste=[]
                        # print(i.strip())
                        token_list.append(i.strip().split("\t"))
                        # token_list.append(liste)
                print("This is Token List:" ,token_list)
                mapped = self.Map(token_list)
                print(mapped)
                port_no = 8002 + id_worker
                push_socket = context.socket(zmq.PUSH)
                push_socket.connect('tcp://127.0.0.1:' + str(port_no) ) 
                push_socket.send_json(mapped)
                break
    

    def ResultCollector(self):
        pid = os.getpid()
        print('Collector: ', pid)
        context = zmq.Context()
        pull_socket = context.socket(zmq.PULL)
        liste=[]
        break_it = 0 
        while True:
            for i in range(self.NumWorkers):
                if len(liste) == self.NumWorkers:
                    break_it = 1 
                    break
                port_no = 8002 + i
                pull_socket.bind('tcp://127.0.0.1:' + str(port_no))
                message = pull_socket.recv_json()
                print(message)
                print("Collector: ", message)
                liste.append(message)
            if break_it == 1:
                break

        curr_max = self.Reduce(liste)
        print(curr_max)