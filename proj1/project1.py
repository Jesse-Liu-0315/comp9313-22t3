import re
from mrjob.job import MRJob
from mrjob.step import MRStep

# ---------------------------------!!! Attention Please!!!------------------------------------
# Please add more details to the comments for each function. Clarifying the input 
# and the output format would be better. It's helpful for tutors to review your code.

# Using multiple MRSteps is permitted, please name your functions properly for readability.

# We will test your code with the following comand:
# "python3 project1.py -r hadoop hdfs_input -o hdfs_output --jobconf mapreduce.job.reduces=2"

# Please make sure that your code can be compiled before submission.
# ---------------------------------!!! Attention Please!!!------------------------------------

class proj1(MRJob):
    #"pair"-order inversion, it is for computing check-in probabilities
    def mapper(self, _, value):
        userID, locID, check_in_time = value.split(",", 2)
        #additionally emits a special key of the form (wi,*)
        yield userID + ",*", 1
        yield userID + "," + locID, 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer_init(self):
        self.marginal = 0
        #stone all the reducer result for reducer_final
        self.resDic = {}
           
    def reducer(self, key, values):
        userID, locID = key.split(",", 1)
        if locID=="*":
            #compute the marginal
            self.marginal = sum(values)
        else:
            count = sum(values)
            # the number of check-ins at location 𝑙𝑜𝑐𝑖 by user 𝑢𝑗 / the number of check-ins from 𝑢𝑗
            res = count/self.marginal
            self.resDic[key] = res
    def reducer_final(self):
        for key, value in self.resDic.items():
            #yield key, str(value)
            userID, locID = key.split(",", 1)
            # in order to send the result to the second job
            yield None, locID + "," + userID + "," + str(value)
    

    # the second job for sorting 
    def mapper_two(self, _, value):
        yield value, None
    def reducer_two_init(self):
        #stone all the result for reducer_final
        self.list = []
    def reducer_two(self, key, values):
        self.list.append(key)
    def reducer_two_final(self):
        for item in self.list:
            locID, userID, value = item.split(",", 2)
            yield locID, userID + "," + value
    
    SORT_VALUES = True
    
    def steps(self):
        JOBCONF = {
            'mapreduce.map.output.key.field.separator':',',
            'mapreduce.partition.keypartitioner.options':'-k1,1',
            'mapreduce.job.output.key.comparator.class':'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            #the sort order is based on the first two field
            'mapreduce.partition.keycomparator.options':'-k1,2'
        }
        JOBCONF2 = {
            'mapreduce.map.output.key.field.separator':',',
            'mapreduce.partition.keypartitioner.options':'-k1,1',
            'mapreduce.job.output.key.comparator.class':'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            # the sort order is first based on the first field(loc) and then based on the third field(check-in probabilities)(reversed)
            'mapreduce.partition.keycomparator.options':'-k1,1 -k3,3nr'
        }

        return [
            MRStep(jobconf=JOBCONF,
                   mapper=self.mapper,
                   combiner= self.combiner,
                   reducer=self.reducer, 
                   reducer_init= self.reducer_init, 
                   reducer_final=self.reducer_final),
            MRStep(jobconf=JOBCONF2,
                   mapper = self.mapper_two,
                   reducer = self.reducer_two,
                   reducer_init= self.reducer_two_init, 
                   reducer_final=self.reducer_two_final)
        ]
    
if __name__ == '__main__':
    proj1.run()