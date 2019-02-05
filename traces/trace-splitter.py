#!/usr/bin/python

import sys, getopt, random

def main(argv):

    num_servers = 9
    num_threads_per_server = 9
    output_path = "./current-splited-traces"
    zipf_exponent_a = 99 #exponent should be multiplied by 100 i.e 99 --> a=0.99

    try:
        opts, args = getopt.getopt(argv,":hs:c:o:w:k:C:")
    except getopt.GetoptError:
        print "-s <#servers> -t <#threads/server> -o <output_path>"
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            print 'Help: '
            print "-s <#servers> -t <#threads/server> -o <output_path>"
            sys.exit()
        elif opt == "-s":
            num_servers = int(arg)
        elif opt == "-t":
            num_threads_per_server = int(arg)
        elif opt == "-o":
            output_path = arg

    files = []
    for s in range(num_servers):
        files.append([])
        for c in range(num_threads_per_server):
            files[s].append(open(output_path + "/s_" + str(s) + "_t_"
                + str(c) + "_a_" + str(zipf_exponent_a) + ".txt",'w'))

    for line in sys.stdin:
        words = line.strip().split()
        if words[0][0] == 'H':
            new_line = words[0] + " " + words[2] + " " + str(hot_keys_2_cores[int(words[3])]) + " " + words[3] + "\n"
        else:
            rand_key = random.randrange(num_keys_per_worker)
            rand_core = random.randrange(num_threads_per_server)
            while rand_key < num_cached_keys and hot_keys_2_cores[rand_key] == rand_core:
                rand_core = random.randrange(num_threads_per_server)
            new_line = words[0] + " " + words[2] + " " + str(rand_core) + " " + str(rand_key) + "\n"
        files[int(words[1])][random.randrange(num_clients_per_server)].write(new_line)

    for server_files in files:
        for f in server_files: 
            f.close()

if __name__ == "__main__":
    main(sys.argv[1:])
