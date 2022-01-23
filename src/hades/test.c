//
// Created by akatsarakis on 21/05/19.
//

#include <getopt.h>
#include "../../include/hades/hades.h"

int
main(int argc, char* argv[])
{
  machine_id = -1;

  static struct option opts[] = {
      {.name = "machine-id", .has_arg = 1, .val = 'm'},
      {.name = "dev-name", .has_arg = 1, .val = 'd'},
      {0}};

  /* Parse and check arguments */
  while (1) {
    int c = getopt_long(argc, argv, "m:d:", opts, NULL);
    if (c == -1) {
      break;
    }
    switch (c) {
      case 'm':
        machine_id = atoi(optarg);
        break;
      case 'd':
        memcpy(dev_name, optarg, strlen(optarg));
        break;
      default:
        printf("Invalid argument %d\n", c);
        assert(false);
    }
  }

  hades_full_thread(&machine_id);
}
