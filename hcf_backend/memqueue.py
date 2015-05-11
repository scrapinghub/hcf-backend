from queuelib import PriorityQueue, queue

class MemQueue(object):
    def __init__(self):
        self.mqclass = queue.FifoMemoryQueue
        self.mq = PriorityQueue(self._mqfactory)

    def _mqfactory(self, priority):
        return self.mqclass()

    def push(self, request):
        return self.mq.push(request)

    def pop(self):
        return self.mq.pop()

    def __len__(self):
        return len(self.mq)

    def close(self):
        self.mq.close()
