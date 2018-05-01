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
        self.hot_reads = []
        self.hot_writes = []
        self.local_reqs = []
        self.remote_reqs = []
        self.all_reqs = []
        self.parseInputStats()
        self.printAllStats()
       # self.printStats(all_reqs)

    def printStats(self, array):
        self.avgLatency(array)
        self.percentileLatency(array, 50)
        #self.percentileLatency(array, 90)
        self.percentileLatency(array, 95)
        self.percentileLatency(array, 99)
        self.percentileLatency(array, 100)

    def printAllStats(self):
        print "~~~~~~ Hot Write Stats ~~~~~~~"
        self.printStats(self.hot_writes)
        print "\n~~~~~~ Hot Read Stats ~~~~~~~~"
        self.printStats(self.hot_reads)
        print "\n~~~~~~ Remote Req Stats ~~~~~~"
        self.printStats(self.remote_reqs)
        print "\n~~~~~~ Local Req Stats ~~~~~~~"
        self.printStats(self.local_reqs)
        print "\n~~~~~~ Overall Stats ~~~~~~~~~"
        self.printStats(self.all_reqs)


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
        hr_lines = 0
        hw_lines = 0
        for line in sys.stdin:                  # input from standard input
            if line[0] == '#':
                continue
            (command, words) = line.strip().split(":",1)
            command = command.strip()
            if command == 'rr':
                words = words.strip().split(",")
                #if int(words[0].strip()) != -1:
                self.latency_values.append(int(words[0].strip()))
                self.remote_reqs.append(int(words[1].strip()))
                self.all_reqs.append(int(words[1].strip()))
            elif command == 'lr':
                words = words.strip().split(",")
                self.local_reqs.append(int(words[1].strip()))
                self.all_reqs[lr_lines] = self.all_reqs[lr_lines] + self.local_reqs[-1]
                lr_lines = lr_lines + 1
            elif command == 'hr':
                words = words.strip().split(",")
                self.hot_reads.append(int(words[1].strip()))
                self.all_reqs[hr_lines] = self.all_reqs[hr_lines] + self.hot_reads[-1]
                hr_lines = hr_lines + 1
            elif command == 'hw':
                words = words.strip().split(",")
                self.hot_writes.append(int(words[1].strip()))
                self.all_reqs[hw_lines] = self.all_reqs[hw_lines] + self.hot_writes[-1]
                hw_lines = hw_lines + 1

if __name__ == '__main__':
    LatencyParser()
