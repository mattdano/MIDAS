import argparse
import random
random.seed(1)

class EdgeHash():
    def __init__(self,num_rows,num_buckets,m):
        self.m = m
        self.num_rows = num_rows
        self.num_buckets = num_buckets

        self.hash_a = [None]* num_rows
        self.hash_b = [None]* num_rows

        for i in range(0,num_rows):
            self.hash_a[i] = random.randrange(1,2147483647) % (num_buckets - 1) + 1
            self.hash_b[i] = random.randrange(1,2147483647) % num_buckets

        self.clear()

    def hash(self,a,b,i):
        resid = ((a + self.m * b) * self.hash_a[i] + self.hash_b[i]) % self.num_buckets
        if resid < 0:
            extra = self.num_buckets
        else:
            extra = 0

        return resid + extra

    def insert(self,a,b,weight):
        for i in range(0,self.num_rows):
            bucket = self.hash(a,b,i)
            self.count[i][bucket] += weight

    def clear(self):
        self.count = [[0.0]*self.num_buckets]*self.num_rows


    def get_count(self,a,b):
        min_count=2**32
        for i in range(0,self.num_rows):
            bucket = self.hash(a,b,i)
            min_count = min(min_count,self.count[i][bucket])

        return min_count

class MIDAS():
    # only midas non temporal implemented so far. MIDAS_R to come.
    def __init__(self,data,num_rows=2,num_buckets=769):
        #   data is a tuple in the form:
        #   (source,dest,time)
        #   time must be presorted, and increasing.
        #   source, dest and time are all int types
        last_t = 0
        for src,dest,t in data:

            if t< last_t:
                print('Error: Time steps should be increasing.')
                raise ValueError
            last_t = t

            #incase you are just loading the dataset from memory and not using the file function...
            if type(src) != int or type(dest) != int or type(t) != int:
                print('Error: I wanted integers!')
                raise TypeError

        self.data = data

        m = len(data)
        self.current_count = EdgeHash(num_rows,num_buckets,m)
        self.total_count = EdgeHash(num_rows,num_buckets,m)



    def calc_anom(self):
        self.anomaly_score = []
        current_time = 1

        for i,(src,dst,timestep) in enumerate(self.data):
            if (i==0) or (timestep > current_time):
                #print('current_count bit clear')
                self.current_count.clear()
                if timestep == 0:
                    current_time = 1
                else:
                    current_time = timestep

            self.current_count.insert(src,dst,1)
            self.total_count.insert(src,dst,1)
            cur_mean = self.total_count.get_count(src, dst) / current_time
            sqerr = pow(self.current_count.get_count(src, dst) - cur_mean,2)

            if current_time == 1:
                current_score = 0
            else:
                current_score = sqerr / cur_mean + sqerr / (cur_mean * (current_time - 1))

            self.anomaly_score.append(current_score)

        return self.anomaly_score

def load_csv(file):
    f = open(file,'r')
    lines = f.readlines()
    f.close()

    results = []
    for line in lines:
        results.append(list(map(int,line.split(','))))

    return results

if __name__ == '__main__':
    results = load_csv('../twitter_security_demo/tweet_processed.csv')
    #results = load_csv('../twitter_security_demo/tweet_processed_small.csv')
    #results = load_csv('./tweet_processed_malformed.csv')
    #print(results)
    import time
    start = time.time()
    myMidas = MIDAS(results)
    scores = myMidas.calc_anom()
    print('took:',time.time()-start)
    f = open('./py_output_test.txt','w')

    for score in scores:
        f.write(str(round(score,6))+'\n')
    f.close()

