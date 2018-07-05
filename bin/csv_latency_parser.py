#!/usr/bin/python

import sys, os, ntpath, getopt

"""
========
Parser 4 aggregated over time results
========
"""
class LatencyParser:
    def __init__(self):
        self.latency_values = []
        self.reads = []
        self.max_read_latency = 0
        self.max_write_latency = 0
        self.writes = []
        self.all_reqs = []
        self.parseInputStats()
        self.printAllStats()
       # self.printStats(all_reqs)

    def printStats(self, array, max_latency):
        self.avgLatency(array)
        self.percentileLatency(array, 50)
        #self.percentileLatency(array, 90)
        self.percentileLatency(array, 95)
        self.percentileLatency(array, 99)
        self.percentileLatency(array, 99.9)
        self.percentileLatency(array, 99.99)
        self.percentileLatency(array, 99.999)
        self.percentileLatency(array, 99.9999)
        self.percentileLatency(array, 100)
        print "Max Latency: ", max_latency, "\n"

    def printAllStats(self):
        print "~~~~~~ Write Stats ~~~~~~~"
        self.printStats(self.writes, self.max_write_latency)
        print "\n~~~~~~ Read Stats ~~~~~~~~"
        self.printStats(self.reads, self.max_read_latency)
        print "\n~~~~~~ Overall Stats ~~~~~~~~~"
        self.printStats(self.all_reqs, max(self.max_read_latency, self.max_write_latency))


    def avgLatency(self, array):
        cummulative = 0 
        total_reqs = 0 
        for x in xrange(len(self.latency_values)):
            cummulative = self.latency_values[x] * array[x] + cummulative 
            total_reqs += array[x]
        if total_reqs > 0:
            print "Reqs measured: ", total_reqs, "| Avg Latency: ", cummulative / total_reqs
        else:
            print "No reqs measured"

    def percentileLatency(self, array, percentage):
        total_reqs = 0
        sum_reqs = 0
        for x in xrange(len(self.latency_values)):
            #cummulative = self.latency_values[x] * array[x] + cummulative 
            total_reqs += array[x]
        if total_reqs > 0:
	    if percentage == 100:
		for x in reversed(xrange(len(self.latency_values))):
			if array[x] > 0:
                		print percentage, "%: ", self.latency_values[x]
		   		return 
	    else:
            	for x in xrange(len(self.latency_values)):
                	sum_reqs += array[x] 
                	if ((100.0 * sum_reqs) / total_reqs) >= percentage:
                    		print percentage, "% : ", self.latency_values[x]
                    		return
        else:
            print "No reqs measured"

    def parseInputStats(self):
        lr_lines = 0
        for line in sys.stdin:                  # input from standard input
            if line[0] == '#':
                continue
            (command, words) = line.strip().split(":",1)
            command = command.strip()
            if command == 'reads':
                words = words.strip().split(",")
                #if int(words[0].strip()) != -1:
                self.latency_values.append(int(words[0].strip()))
                self.reads.append(int(words[1].strip()))
                self.all_reqs.append(int(words[1].strip()))
            elif command == 'writes':
                words = words.strip().split(",")
                self.writes.append(int(words[1].strip()))
                self.all_reqs[lr_lines] = self.all_reqs[lr_lines] + self.writes[-1]
                lr_lines = lr_lines + 1
            elif command == 'reads-hl':
                words = words.strip().split(",")
                self.max_read_latency = int(words[0].strip())
            elif command == 'writes-hl':
                words = words.strip().split(",")
                self.max_write_latency = int(words[0].strip())

if __name__ == '__main__':
    LatencyParser()
