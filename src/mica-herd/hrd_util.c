#include "hrd.h"

/* Every thread creates a TCP connection to the registry only once. */
__thread memcached_st *memc = NULL;


/*
 * Finds the port with rank `port_index` (0-based) in the list of ENABLED ports.
 * Fills its device id and device-local port id (1-based) into the supplied
 * control block.
 */

char dev_name[50];
struct ibv_device*
hrd_resolve_port_index(struct hrd_ctrl_blk *cb, int port_index)
{
	struct ibv_device **dev_list;
	int num_devices = 0;

	assert(port_index >= 0);

	cb->device_id = -1;
	cb->dev_port_id = -1;

	dev_list = ibv_get_device_list(&num_devices);
	CPE(!dev_list, "HRD: Failed to get IB devices list", 0);

	int ports_to_discover = port_index;
	int dev_i;

	for(dev_i = 0; dev_i < num_devices; dev_i++) {
		if (strcmp(dev_list[dev_i]->name, dev_name) != 0) continue;

		struct ibv_context *ctx = ibv_open_device(dev_list[dev_i]);
		CPE(!ctx, "HRD: Couldn't open device", 0);

		struct ibv_device_attr device_attr;
		memset(&device_attr, 0, sizeof(device_attr));
		if(ibv_query_device(ctx, &device_attr)) {
			printf("HRD: Could not query device: %d\n", dev_i);
			exit(-1);
		}

		uint8_t port_i;
		for(port_i = 1; port_i <= device_attr.phys_port_cnt; port_i++) {

			/* Count this port only if it is enabled */
			struct ibv_port_attr port_attr;
			if(ibv_query_port(ctx, port_i, &port_attr) != 0) {
				printf("HRD: Could not query port %d of device %d\n",
					port_i, dev_i);
				exit(-1);
			}

			if(port_attr.phys_state != IBV_PORT_ACTIVE &&
				port_attr.phys_state != IBV_PORT_ACTIVE_DEFER) {
#ifndef __cplusplus
				printf("HRD: Ignoring port %d of device %d. State is %s\n",
					port_i, dev_i, ibv_port_state_str(port_attr.phys_state));
#else
				printf("HRD: Ignoring port %d of device %d. State is %s\n",
					port_i, dev_i,
					ibv_port_state_str((ibv_port_state) port_attr.phys_state));
#endif
				continue;
			}

			if(ports_to_discover == 0) {
				// printf("HRD: port index %d resolved to device %d, port %d\n",
				// 	port_index, dev_i, port_i);

				/* Fill the device ID and device-local port ID */
				cb->device_id = dev_i;
				cb->dev_port_id = port_i;

				if (ibv_close_device(ctx)) {
					fprintf(stderr, "HRD: Couldn't release context\n");
					assert(false);
				}

				return dev_list[cb->device_id];
			}

			ports_to_discover--;
		}

		if (ibv_close_device(ctx)) {
			fprintf(stderr, "HRD: Couldn't release context\n");
			assert(false);
		}
	}

	/* If we come here, port resolution failed */
	assert(cb->device_id == -1 && cb->dev_port_id == -1);
	fprintf(stderr, "HRD: Invalid port index %d. Exiting.\n", port_index);
	exit(-1);
}

/* Allocate SHM with @shm_key, and save the shmid into @shm_id_ret */
void* hrd_malloc_socket(int shm_key, uint64_t size, int socket_id)
{
    int shmid;
    int shm_flag = IPC_CREAT | IPC_EXCL | 0666 |
                   (USE_HUGE_PAGES == 1 ? SHM_HUGETLB : 0);

    shmid = shmget(shm_key, size, shm_flag);

    if(shmid == -1) {
        switch(errno) {
            case EACCES:
                red_printf("HRD: SHM malloc error: Insufficient permissions."
                           " (SHM key = %d)\n", shm_key);
                break;
            case EEXIST:
                red_printf("HRD: SHM malloc error: Already exists."
                           " (SHM key = %d)\n", shm_key);
                break;
            case EINVAL:
                red_printf("HRD: SHM malloc error: SHMMAX/SHMIN mismatch."
                           " (SHM key = %d, size = %lu)\n", shm_key, size);
                break;
            case ENOMEM:
                red_printf("HRD: SHM malloc error: Insufficient memory."
                           " (SHM key = %d, size = %lu)\n", shm_key, size);
                break;
            case ENOENT:
                red_printf("HRD: SHM malloc error: No segment exists for the given key, and IPC_CREAT was not specified."
                           " (SHM key = %d, size = %lu)\n", shm_key, size);
                break;
            case ENOSPC:
                red_printf(
                        "HRD: SHM malloc error: All possible shared memory IDs have been taken or the limit of shared memory is exceeded."
                        " (SHM key = %d, size = %lu)\n", shm_key, size);
                break;
            case EPERM:
                red_printf("HRD: SHM malloc error: The SHM_HUGETLB flag was specified, but the caller was not privileged"
                           " (SHM key = %d, size = %lu)\n", shm_key, size);
                break;
            case ENFILE:
                red_printf("HRD: SHM malloc error: The system-wide limit on the total number of open files has been reached."
                           " (SHM key = %d, size = %lu)\n", shm_key, size);
                break;
            default:
                red_printf("HRD: SHM malloc error: A wild SHM error: %s.\n",
                           strerror(errno));
                break;
        }
        assert(false);
    }

	void *buf = shmat(shmid, NULL, 0);
	if(buf == NULL) {
		printf("HRD: SHM malloc error: shmat() failed for key %d\n", shm_key);
		exit(-1);
	}

	/* Bind the buffer to this socket */
	const unsigned long nodemask = (1 << socket_id);
	int ret = mbind(buf, size, MPOL_BIND, &nodemask, 32, 0);
	if(ret != 0) {
		printf("HRD: SHM malloc error. mbind() failed for key %d\n", shm_key);
		exit(-1);
	}

	// vasilis- try to take advantage of TLB coalescing, if it is there
	if (LEVERAGE_TLB_COALESCING) {
		uint64_t page_no = CEILING(size, HUGE_PAGE_SIZE) - 1;
		for (uint64_t i = 0; i < page_no; i++){
            uint8_t *buf_ptr = ((uint8_t*) buf) + (i * HUGE_PAGE_SIZE);
            memset(buf_ptr, 0, 1);
		}
	}

	return buf;
}

/* Free shm @shm_key and @shm_buf. Return 0 on success, else -1. */
int hrd_free(int shm_key, void *shm_buf)
{
	int ret;
	int shmid = shmget(shm_key, 0, 0);
	if(shmid == -1) {
		switch(errno) {
			case EACCES:
				printf("HRD: SHM free error: Insufficient permissions."
					" (SHM key = %d)\n", shm_key);
				break;
			case ENOENT:
				printf("HRD: SHM free error: No such SHM key. (SHM key = %d)\n",
					shm_key);
				break;
			default:
				printf("HRD: SHM free error: A wild SHM error: %s\n",
					strerror(errno));
				break;
		}
		return -1;
	}

	ret = shmctl(shmid, IPC_RMID, NULL);
	if(ret != 0) {
		printf("HRD: SHM free error: shmctl() failed for key %d\n", shm_key);
		exit(-1);
	}

	ret = shmdt(shm_buf);
	if(ret != 0) {
		printf("HRD: SHM free error: shmdt() failed for key %d\n", shm_key);
		exit(-1);
	}

	return 0;
}

/* Get the LID of a port on the device specified by @ctx */
uint16_t hrd_get_local_lid(struct ibv_context *ctx, int dev_port_id)
{
	assert(ctx != NULL && dev_port_id >= 1);

	struct ibv_port_attr attr;
	if(ibv_query_port(ctx, dev_port_id, &attr)) {
		printf("HRD: ibv_query_port on port %d of device %s failed! Exiting.\n",
			dev_port_id, ibv_get_device_name(ctx->device));
		assert(false);
	}

	return attr.lid;
}

/* Return the environment variable @name if it is set. Exit if not. */
char* hrd_getenv(const char *name)
{
	char *env = getenv(name);
	if(env == NULL) {
		fprintf(stderr, "Environment variable %s not set\n", name);
		assert(false);
	}

	return env;
}

memcached_st* hrd_create_memc()
{
	memcached_server_st *servers = NULL;
	memcached_st *memc = memcached_create(NULL);
	memcached_return rc;
	memc = memcached_create(NULL);

	char *registry_ip = hrd_getenv("HRD_REGISTRY_IP");
//	printf("Appending server with IP: %s \n", registry_ip);
	servers = memcached_server_list_append(servers,
										   registry_ip, MEMCACHED_DEFAULT_PORT, &rc);
	// Pushes an array of memcached_server_st into the memcached_st structure.
	//These servers will be placed at the end.
	rc = memcached_server_push(memc, servers);
	CPE(rc != MEMCACHED_SUCCESS, "Couldn't add memcached server.\n", -1);

	return memc;
}

/*
 * Insert key -> value mapping into memcached running at HRD_REGISTRY_IP.
 */
void hrd_publish(const char *key, void *value, int len)
{
	assert(key != NULL && value != NULL && len > 0);
	memcached_return rc;

	if(memc == NULL) {
		memc = hrd_create_memc();
	}

	rc = memcached_set(memc, key, strlen(key), (const char *) value, len,
		(time_t) 0, (uint32_t) 0);
	if (rc != MEMCACHED_SUCCESS) {
		char *registry_ip = hrd_getenv("HRD_REGISTRY_IP");
		fprintf(stderr, "\tHRD: Failed to publish key %s to memcached. Error %s. "
			"Reg IP = %s\n", key, memcached_strerror(memc, rc), registry_ip);
		exit(-1);
	}
}

/*
 * Get the value associated with "key" into "value", and return the length
 * of the value. If the key is not found, return NULL and len -1. For all
 * other errors, terminate.
 *
 * This function sometimes gets called in a polling loop - ensure that there
 * are no memory leaks or unterminated memcached connections! We don't need
 * to free() the resul of getenv() since it points to a string in the process
 * environment.
 */
int hrd_get_published(const char *key, void **value)
{
	assert(key != NULL);
	if(memc == NULL) {
		memc = hrd_create_memc();
	}

	memcached_return rc;
	size_t value_length;
	uint32_t flags;

	*value = memcached_get(memc, key, strlen(key), &value_length, &flags, &rc);

	if(rc == MEMCACHED_SUCCESS ) {
		return (int) value_length;
	} else if (rc == MEMCACHED_NOTFOUND) {
		assert(*value == NULL);
		return -1;
	} else {
		char *registry_ip = hrd_getenv("HRD_REGISTRY_IP");
		//char *registry_ip = is_client == 1 ? remote_IP : local_IP;
		fprintf(stderr, "HRD: Error finding value for key \"%s\": %s. "
			"Reg IP = %s\n", key, memcached_strerror(memc, rc), registry_ip);
		exit(-1);
	}

	/* Never reached */
	assert(false);
}




/* Like printf, but red. Limited to 1000 characters. */
void red_printf(const char *format, ...)
{
#define RED_LIM 1000
    va_list args;
    int i;

    char buf1[RED_LIM], buf2[RED_LIM];
    memset(buf1, 0, RED_LIM);
    memset(buf2, 0, RED_LIM);

    va_start(args, format);

    /* Marshal the stuff to print in a buffer */
    vsnprintf(buf1, RED_LIM, format, args);

    /* Probably a bad check for buffer overflow */
    for(i = RED_LIM - 1; i >= RED_LIM - 50; i --) {
        assert(buf1[i] == 0);
    }

    /* Add markers for red color and reset color */
    // snprintf(buf2, 1000, "\033[31m%s\033[0m", buf1);
    snprintf(buf2, 1000, "\033[31m%s\033[0m", buf1);

    /* Probably another bad check for buffer overflow */
    for(i = RED_LIM - 1; i >= RED_LIM - 50; i --) {
        assert(buf2[i] == 0);
    }

    printf("%s", buf2);

    va_end(args);
}

/* Like printf, but yellow. Limited to 1000 characters. */
void yellow_printf(const char *format, ...)
{
#define RED_LIM 1000
    va_list args;
    int i;

    char buf1[RED_LIM], buf2[RED_LIM];
    memset(buf1, 0, RED_LIM);
    memset(buf2, 0, RED_LIM);

    va_start(args, format);

    /* Marshal the stuff to print in a buffer */
    vsnprintf(buf1, RED_LIM, format, args);

    /* Probably a bad check for buffer overflow */
    for(i = RED_LIM - 1; i >= RED_LIM - 50; i --) {
        assert(buf1[i] == 0);
    }

    /* Add markers for yellow color and reset color */
    // snprintf(buf2, 1000, "\033[31m%s\033[0m", buf1);
    snprintf(buf2, 1000, "\033[33m%s\033[0m", buf1);

    /* Probably another bad check for buffer overflow */
    for(i = RED_LIM - 1; i >= RED_LIM - 50; i --) {
        assert(buf2[i] == 0);
    }

    printf("%s", buf2);

    va_end(args);
}

/* Like printf, but green. Limited to 1000 characters. */
void green_printf(const char *format, ...)
{
#define RED_LIM 1000
    va_list args;
    int i;

    char buf1[RED_LIM], buf2[RED_LIM];
    memset(buf1, 0, RED_LIM);
    memset(buf2, 0, RED_LIM);

    va_start(args, format);

    /* Marshal the stuff to print in a buffer */
    vsnprintf(buf1, RED_LIM, format, args);

    /* Probably a bad check for buffer overflow */
    for(i = RED_LIM - 1; i >= RED_LIM - 50; i --) {
        assert(buf1[i] == 0);
    }

    /* Add markers for green color and reset color */
    snprintf(buf2, 1000, "\033[1m\033[32m%s\033[0m", buf1);

    /* Probably another bad check for buffer overflow */
    for(i = RED_LIM - 1; i >= RED_LIM - 50; i --) {
        assert(buf2[i] == 0);
    }

    printf("%s", buf2);

    va_end(args);
}

/* Like printf, but magenta. Limited to 1000 characters. */
void cyan_printf(const char *format, ...)
{
#define RED_LIM 1000
    va_list args;
    int i;

    char buf1[RED_LIM], buf2[RED_LIM];
    memset(buf1, 0, RED_LIM);
    memset(buf2, 0, RED_LIM);

    va_start(args, format);

    /* Marshal the stuff to print in a buffer */
    vsnprintf(buf1, RED_LIM, format, args);

    /* Probably a bad check for buffer overflow */
    for(i = RED_LIM - 1; i >= RED_LIM - 50; i --) {
        assert(buf1[i] == 0);
    }

    /* Add markers for magenta color and reset color */
    snprintf(buf2, 1000, "\033[1m\033[36m%s\033[0m", buf1);

    /* Probably another bad check for buffer overflow */
    for(i = RED_LIM - 1; i >= RED_LIM - 50; i --) {
        assert(buf2[i] == 0);
    }

    printf("%s", buf2);

    va_end(args);
}

