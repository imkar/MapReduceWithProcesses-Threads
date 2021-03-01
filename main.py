# from MapReduce import MapReduce
import sys  
from FindCitations import FindCitations
from FindCyclicReferences import FindCyclicReferences

if __name__ == "__main__":     
    type_ = sys.argv[1]
    worker_num = int(sys.argv[2])     
    filename = sys.argv[3]

    if type_ == 'COUNT':
        mr = FindCitations(worker_num)
        mr.start(filename)
    elif type_ == 'CYCLE':
        mr = FindCyclicReferences(worker_num)
        mr.start(filename)
    else:
        print("INVALID COMMAND!")