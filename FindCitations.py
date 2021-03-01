from MapReduce import MapReduce

class FindCitations(MapReduce):
    def Map(self, parts):
        d = dict()
        for i in parts:
            # [[id,1231],[]]
            d[i[1]] = 0
        for i in parts:
            d[i[1]] += 1
        return d
    # [{'id1':1,'id3':4},{},{}]
    def Reduce(self, kvs):
        # d = {'id1':5,'id2':6}
        # d = {'id1':8,'id2':8}
        dall = {}
        for i in kvs:
            dall.update(i)
        for k,v in dall.items():
            dall[k] = 0
        for i in kvs:
            for k,v in i.items():
                dall[k] += v
        f = open("results.txt", 'w')
        f.write(str(dall))
        f.close()
        return dall