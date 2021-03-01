from MapReduce import MapReduce

class FindCyclicReferences(MapReduce):
    def Map(self, parts):
        # [[1,2],[a,b],[b,a]]
        d = dict()
        for i in parts:
            if i[0] < i[1]:
                tupl = '(' + str(i[0]) +','+str(i[1])+')'
            else:
                tupl = '(' + str(i[1]) +','+str(i[0])+')'
            if tupl in d:
                d[tupl] = 1
            else:
                d[tupl] = 0
        return d

    def Reduce(self, kvs):
        # [{'id1':1,'id3':4},{},{}]
        # 1,2 : 0     2,3: 1      1,4: 0
        # 1,2: 0  1,5: 0
        dall = dict()
        dresult = dict()
        for i in kvs:
            for k,v in i.items():
                if k in dall:
                    dall[k] = 1
                else:
                    if v:
                        dall[k] = 1
                    else:
                        dall[k] = 0
        for k, v in dall.items():
            if v:
                dresult[k] = v
        f = open("results.txt", 'w')
        f.write(str(dresult))
        f.close()
        return dresult
