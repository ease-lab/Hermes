#!/usr/bin/python

import sys, getopt, random

def main(argv):

    num_servers = 9
    num_clients_per_server = 23
    num_workers_per_server = 9
    output_path = "./current-splited-traces"
    num_keys_per_worker = 4 * 1024 * 1024
    num_cached_keys = 250000 
    zipf_exponent_a = 99 #exponent should be multiplied by 100 i.e 99 --> a=0.99

    try:
        opts, args = getopt.getopt(argv,":hs:c:o:w:k:C:")
    except getopt.GetoptError:
        print "-s <#servers> -c <#clients/server> -w <#workers/server>"
        print "-k <#keys/worker> -C <#cached keys> -o <output_path>"
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            print 'Help: '
            print "-s <#servers> -c <#clients/server> -w <#workers/server>"
            print "-k <#keys/worker> -C <#cached keys> -o <output_path>"
            sys.exit()
        elif opt == "-s":
            num_servers = int(arg)
        elif opt == "-c":
            num_clients_per_server = int(arg)
        elif opt == "-w":
            num_workers_per_server = int(arg)
        elif opt == "-k":
            num_keys_per_worker = int(arg)
        elif opt == "-C":
            num_cached_keys = int(arg)
        elif opt == "-o":
            output_path = arg

    files = []
    for s in range(num_servers):
        files.append([])
        for c in range(num_clients_per_server):
            files[s].append(open(output_path + "/s_" + str(s) + "_c_"
                + str(c) + "_a_" + str(zipf_exponent_a) + ".txt",'w')) 

    hot_keys_2_cores = []
    for key in range(num_cached_keys):
	#print "Key: ", key
        hot_keys_2_cores.append(random.randrange(num_workers_per_server))

    for line in sys.stdin:
        words = line.strip().split()
        if words[0][0] == 'H':
            new_line = words[0] + " " + words[2] + " " + str(hot_keys_2_cores[int(words[3])]) + " " + words[3] + "\n"
        else:
            rand_key = random.randrange(num_keys_per_worker)
            rand_core = random.randrange(num_workers_per_server)
            while rand_key < num_cached_keys and hot_keys_2_cores[rand_key] == rand_core:
                rand_core = random.randrange(num_workers_per_server)
            new_line = words[0] + " " + words[2] + " " + str(rand_core) + " " + str(rand_key) + "\n"
        files[int(words[1])][random.randrange(num_clients_per_server)].write(new_line)

    for server_files in files:
        for f in server_files: 
            f.close()

if __name__ == "__main__":
    main(sys.argv[1:])
