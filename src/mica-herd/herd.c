#include "hrd.h"

/* Every thread creates a TCP connection to the registry only once. */
__thread memcached_st* memc = NULL;

/*
 * Finds the port with rank `port_index` (0-based) in the list of ENABLED ports.
 * Fills its device id and device-local port id (1-based) into the supplied
 * control block.
 */

char dev_name[50];
struct ibv_device*
hrd_resolve_port_index(struct hrd_ud_ctrl_blk* cb, int port_index)
{
  struct ibv_device** dev_list;
  int num_devices = 0;

  assert(port_index >= 0);

  cb->device_id = -1;
  cb->dev_port_id = -1;

  dev_list = ibv_get_device_list(&num_devices);
  CPE(!dev_list, "HRD: Failed to get IB devices list", 0);

  int ports_to_discover = port_index;
  int dev_i;

  for (dev_i = 0; dev_i < num_devices; dev_i++) {
    if (strcmp(dev_list[dev_i]->name, dev_name) != 0) continue;

    struct ibv_context* ctx = ibv_open_device(dev_list[dev_i]);
    CPE(!ctx, "HRD: Couldn't open device", 0);

    struct ibv_device_attr device_attr;
    memset(&device_attr, 0, sizeof(device_attr));
    if (ibv_query_device(ctx, &device_attr)) {
      printf("HRD: Could not query device: %d\n", dev_i);
      exit(-1);
    }

    uint8_t port_i;
    for (port_i = 1; port_i <= device_attr.phys_port_cnt; port_i++) {
      /* Count this port only if it is enabled */
      struct ibv_port_attr port_attr;
      if (ibv_query_port(ctx, port_i, &port_attr) != 0) {
        printf("HRD: Could not query port %d of device %d\n", port_i, dev_i);
        exit(-1);
      }

      if (port_attr.phys_state != IBV_PORT_ACTIVE &&
          port_attr.phys_state != IBV_PORT_ACTIVE_DEFER) {
#ifndef __cplusplus
        printf("HRD: Ignoring port %d of device %d. State is %s\n", port_i,
               dev_i, ibv_port_state_str(port_attr.phys_state));
#else
        printf("HRD: Ignoring port %d of device %d. State is %s\n", port_i,
               dev_i, ibv_port_state_str((ibv_port_state)port_attr.phys_state));
#endif
        continue;
      }

      if (ports_to_discover == 0) {
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
void*
hrd_malloc_socket(int shm_key, uint64_t size, int socket_id)
{
  int shmid;
  int shm_flag =
      IPC_CREAT | IPC_EXCL | 0666 | (USE_HUGE_PAGES == 1 ? SHM_HUGETLB : 0);

  shmid = shmget(shm_key, size, shm_flag);

  if (shmid == -1) {
    switch (errno) {
      case EACCES:
        colored_printf(RED,
                       "HRD: SHM malloc error: Insufficient permissions."
                       " (SHM key = %d)\n",
                       shm_key);
        break;
      case EEXIST:
        colored_printf(RED,
                       "HRD: SHM malloc error: Already exists."
                       " (SHM key = %d)\n",
                       shm_key);
        break;
      case EINVAL:
        colored_printf(RED,
                       "HRD: SHM malloc error: SHMMAX/SHMIN mismatch."
                       " (SHM key = %d, size = %lu)\n",
                       shm_key, size);
        break;
      case ENOMEM:
        colored_printf(RED,
                       "HRD: SHM malloc error: Insufficient memory."
                       " (SHM key = %d, size = %lu)\n",
                       shm_key, size);
        break;
      case ENOENT:
        colored_printf(RED,
                       "HRD: SHM malloc error: No segment exists for the given "
                       "key, and IPC_CREAT was not specified."
                       " (SHM key = %d, size = %lu)\n",
                       shm_key, size);
        break;
      case ENOSPC:
        colored_printf(
            RED,
            "HRD: SHM malloc error: All possible shared memory IDs have been "
            "taken or the limit of shared memory is exceeded."
            " (SHM key = %d, size = %lu)\n",
            shm_key, size);
        break;
      case EPERM:
        colored_printf(RED,
                       "HRD: SHM malloc error: The SHM_HUGETLB flag was "
                       "specified, but the caller was not privileged"
                       " (SHM key = %d, size = %lu)\n",
                       shm_key, size);
        break;
      case ENFILE:
        colored_printf(RED,
                       "HRD: SHM malloc error: The system-wide limit on the "
                       "total number of open files has been reached."
                       " (SHM key = %d, size = %lu)\n",
                       shm_key, size);
        break;
      default:
        colored_printf(RED, "HRD: SHM malloc error: A wild SHM error: %s.\n",
                       strerror(errno));
        break;
    }
    assert(false);
  }

  void* buf = shmat(shmid, NULL, 0);
  if (buf == NULL) {
    printf("HRD: SHM malloc error: shmat() failed for key %d\n", shm_key);
    exit(-1);
  }

  /* Bind the buffer to this socket */
  const unsigned long nodemask = (1 << socket_id);
  int ret = mbind(buf, size, MPOL_BIND, &nodemask, 32, 0);
  if (ret != 0) {
    printf("HRD: SHM malloc error. mbind() failed for key %d\n", shm_key);
    exit(-1);
  }

  // vasilis- try to take advantage of TLB coalescing, if it is there
  if (LEVERAGE_TLB_COALESCING) {
    uint64_t page_no = CEILING(size, HUGE_PAGE_SIZE) - 1;
    for (uint64_t i = 0; i < page_no; i++) {
      uint8_t* buf_ptr = ((uint8_t*)buf) + (i * HUGE_PAGE_SIZE);
      memset(buf_ptr, 0, 1);
    }
  }

  return buf;
}

/* Free shm @shm_key and @shm_buf. Return 0 on success, else -1. */
int
hrd_free(int shm_key, void* shm_buf)
{
  int ret;
  int shmid = shmget(shm_key, 0, 0);
  if (shmid == -1) {
    switch (errno) {
      case EACCES:
        printf(
            "HRD: SHM free error: Insufficient permissions."
            " (SHM key = %d)\n",
            shm_key);
        break;
      case ENOENT:
        printf("HRD: SHM free error: No such SHM key. (SHM key = %d)\n",
               shm_key);
        break;
      default:
        printf("HRD: SHM free error: A wild SHM error: %s\n", strerror(errno));
        break;
    }
    return -1;
  }

  ret = shmctl(shmid, IPC_RMID, NULL);
  if (ret != 0) {
    printf("HRD: SHM free error: shmctl() failed for key %d\n", shm_key);
    exit(-1);
  }

  ret = shmdt(shm_buf);
  if (ret != 0) {
    printf("HRD: SHM free error: shmdt() failed for key %d\n", shm_key);
    exit(-1);
  }

  return 0;
}

/* Get the LID of a port on the device specified by @ctx */
uint16_t
hrd_get_local_lid(struct ibv_context* ctx, int dev_port_id)
{
  assert(ctx != NULL && dev_port_id >= 1);

  struct ibv_port_attr attr;
  if (ibv_query_port(ctx, dev_port_id, &attr)) {
    printf("HRD: ibv_query_port on port %d of device %s failed! Exiting.\n",
           dev_port_id, ibv_get_device_name(ctx->device));
    assert(false);
  }

  return attr.lid;
}

/* Return the environment variable @name if it is set. Exit if not. */
char*
hrd_getenv(const char* name)
{
  char* env = getenv(name);
  if (env == NULL) {
    fprintf(stderr, "Environment variable %s not set\n", name);
    assert(false);
  }

  return env;
}

memcached_st*
hrd_create_memc()
{
  memcached_server_st* servers = NULL;
  memcached_st* memc = memcached_create(NULL);
  memcached_return rc;
  memc = memcached_create(NULL);

  char* registry_ip = hrd_getenv("HRD_REGISTRY_IP");
  //	printf("Appending server with IP: %s \n", registry_ip);
  servers = memcached_server_list_append(servers, registry_ip,
                                         MEMCACHED_DEFAULT_PORT, &rc);
  // Pushes an array of memcached_server_st into the memcached_st structure.
  // These servers will be placed at the end.
  rc = memcached_server_push(memc, servers);
  CPE(rc != MEMCACHED_SUCCESS, "Couldn't add memcached server.\n", -1);

  return memc;
}

/*
 * Insert key -> value mapping into memcached running at HRD_REGISTRY_IP.
 */
void
hrd_publish(const char* key, void* value, int len)
{
  assert(key != NULL && value != NULL && len > 0);
  memcached_return rc;

  if (memc == NULL) {
    memc = hrd_create_memc();
  }

  rc = memcached_set(memc, key, strlen(key), (const char*)value, len, (time_t)0,
                     (uint32_t)0);
  if (rc != MEMCACHED_SUCCESS) {
    char* registry_ip = hrd_getenv("HRD_REGISTRY_IP");
    fprintf(stderr,
            "\tHRD: Failed to publish key %s to memcached. Error %s. "
            "Reg IP = %s\n",
            key, memcached_strerror(memc, rc), registry_ip);
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
int
hrd_get_published(const char* key, void** value)
{
  assert(key != NULL);
  if (memc == NULL) {
    memc = hrd_create_memc();
  }

  memcached_return rc;
  size_t value_length;
  uint32_t flags;

  *value = memcached_get(memc, key, strlen(key), &value_length, &flags, &rc);

  if (rc == MEMCACHED_SUCCESS) {
    return (int)value_length;
  } else if (rc == MEMCACHED_NOTFOUND) {
    assert(*value == NULL);
    return -1;
  } else {
    char* registry_ip = hrd_getenv("HRD_REGISTRY_IP");
    // char *registry_ip = is_client == 1 ? remote_IP : local_IP;
    fprintf(stderr,
            "HRD: Error finding value for key \"%s\": %s. "
            "Reg IP = %s\n",
            key, memcached_strerror(memc, rc), registry_ip);
    exit(-1);
  }

  /* Never reached */
  assert(false);
}

/*
 * If @prealloc_conn_buf != NULL, @conn_buf_size is the size of the preallocated
 * buffer. If @prealloc_conn_buf == NULL, @conn_buf_size is the size of the
 * new buffer to create.
 */
struct hrd_ud_ctrl_blk*
hrd_ud_ctrl_blk_init(int local_hid, int port_index,
                     int numa_node_id, /* -1 means don't use hugepages */
                     int num_dgram_qps, int dgram_buf_size,
                     int dgram_buf_shm_key, int* recv_q_depth,
                     int* send_q_depth)
{
  // colored_printf(RED,"HRD: creating control block %d: port %d, socket %d, "
  // 	"conn qps %d, UC %d, conn buf %d bytes (key %d), "
  // 	"dgram qps %d, dgram buf %d bytes (key %d)\n",
  // 	local_hid, port_index, numa_node_id,
  // 	num_conn_qps, use_uc, conn_buf_size, conn_buf_shm_key,
  // 	num_dgram_qps, dgram_buf_size, dgram_buf_shm_key);

  /*
   * Check arguments for sanity.
   * @local_hid can be anything: it's just control block identifier that is
   * useful in printing debug info.
   */
  assert(port_index >= 0 && port_index <= 16);
  assert(numa_node_id >= -1 && numa_node_id <= 8);
  assert(num_dgram_qps >= 0 && num_dgram_qps <= M_2);
  assert(dgram_buf_size >= 0 && dgram_buf_size <= M_1024);

  if (num_dgram_qps == 0) {
    colored_printf(RED,
                   "HRD: Control block initialization without QPs. Are you"
                   " sure you want to do this?\n");
    assert(false);
  }

  struct hrd_ud_ctrl_blk* cb =
      (struct hrd_ud_ctrl_blk*)malloc(sizeof(struct hrd_ud_ctrl_blk));
  memset(cb, 0, sizeof(struct hrd_ud_ctrl_blk));

  /* Fill in the control block */
  cb->local_hid = local_hid;
  cb->numa_node_id = numa_node_id;

  cb->num_dgram_qps = num_dgram_qps;
  cb->dgram_buf_shm_key = dgram_buf_shm_key;

  cb->recv_q_depth = recv_q_depth;
  cb->send_q_depth = send_q_depth;

  /* Get the device to use. This fills in cb->device_id and cb->dev_port_id */
  struct ibv_device* ib_dev = hrd_resolve_port_index(cb, port_index);
  CPE(!ib_dev, "HRD: IB device not found", 0);

  /* Use a single device context and PD for all QPs */
  cb->ctx = ibv_open_device(ib_dev);
  CPE(!cb->ctx, "HRD: Couldn't get context", 0);

  cb->pd = ibv_alloc_pd(cb->ctx);
  CPE(!cb->pd, "HRD: Couldn't allocate PD", 0);

  int ib_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                 IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

  /*
   * Create datagram QPs and transition them RTS.
   * Create and register datagram RDMA buffer.
   */
  if (num_dgram_qps >= 1) {
    cb->dgram_qp =
        (struct ibv_qp**)malloc(num_dgram_qps * sizeof(struct ibv_qp*));
    cb->dgram_send_cq =
        (struct ibv_cq**)malloc(num_dgram_qps * sizeof(struct ibv_cq*));
    cb->dgram_recv_cq =
        (struct ibv_cq**)malloc(num_dgram_qps * sizeof(struct ibv_cq*));

    assert(cb->dgram_qp != NULL && cb->dgram_send_cq != NULL &&
           cb->dgram_recv_cq != NULL);

    hrd_create_dgram_qps(cb);

    /* Create and register dgram_buf */
    int reg_size = 0;

    if (numa_node_id >= 0) {
      /* Hugepages */
      while (reg_size < dgram_buf_size) {
        reg_size += M_2;
      }

      /* SHM key 0 is hard to free later */
      assert(dgram_buf_shm_key >= 1 && dgram_buf_shm_key <= 128);
      cb->dgram_buf = (volatile uint8_t*)hrd_malloc_socket(
          dgram_buf_shm_key, reg_size, numa_node_id);
    } else {
      reg_size = dgram_buf_size;
      cb->dgram_buf = (volatile uint8_t*)memalign(4096, reg_size);
    }

    assert(cb->dgram_buf != NULL);
    memset((char*)cb->dgram_buf, 0, reg_size);

    cb->dgram_buf_mr =
        ibv_reg_mr(cb->pd, (char*)cb->dgram_buf, reg_size, ib_flags);
    assert(cb->dgram_buf_mr != NULL);
  }

  return cb;
}

/* Free up the resources taken by @cb. Return -1 if something fails, else 0. */
int
hrd_ud_ctrl_blk_destroy(struct hrd_ud_ctrl_blk* cb)
{
  int i;
  colored_printf(RED, "HRD: Destroying control block %d\n", cb->local_hid);

  /* Destroy QPs and CQs. QPs must be destroyed before CQs. */
  for (i = 0; i < cb->num_dgram_qps; i++) {
    assert(cb->dgram_send_cq[i] != NULL && cb->dgram_recv_cq[i] != NULL);
    assert(cb->dgram_qp[i] != NULL);

    if (ibv_destroy_qp(cb->dgram_qp[i])) {
      fprintf(stderr, "HRD: Couldn't destroy dgram QP %d\n", i);
      return -1;
    } else
      assert(0);
  }

  /* Destroy memory regions */
  if (cb->num_dgram_qps > 0) {
    assert(cb->dgram_buf_mr != NULL && cb->dgram_buf != NULL);
    if (ibv_dereg_mr(cb->dgram_buf_mr)) {
      fprintf(stderr, "HRD: Couldn't deregister dgram MR for cb %d\n",
              cb->local_hid);
      return -1;
    }

    if (cb->numa_node_id >= 0) {
      if (hrd_free(cb->dgram_buf_shm_key, (void*)cb->dgram_buf)) {
        fprintf(stderr, "HRD: Error freeing dgram hugepages for cb %d\n",
                cb->local_hid);
      }
    } else {
      free((void*)cb->dgram_buf);
    }
  }

  /* Destroy protection domain */
  if (ibv_dealloc_pd(cb->pd)) {
    fprintf(stderr, "HRD: Couldn't dealloc PD for cb %d\n", cb->local_hid);
    return -1;
  }

  /* Destroy device context */
  if (ibv_close_device(cb->ctx)) {
    fprintf(stderr, "Couldn't release context for cb %d\n", cb->local_hid);
    return -1;
  }

  colored_printf(RED, "HRD: Control block %d destroyed.\n", cb->local_hid);
  return 0;
}

/* Create datagram QPs and transition them to RTS */
void
hrd_create_dgram_qps(struct hrd_ud_ctrl_blk* cb)
{
  int i;
  assert(cb->dgram_qp != NULL && cb->dgram_send_cq != NULL &&
         cb->dgram_recv_cq != NULL && cb->pd != NULL && cb->ctx != NULL);
  assert(cb->num_dgram_qps >= 1 && cb->dev_port_id >= 1);

  for (i = 0; i < cb->num_dgram_qps; i++) {
    cb->dgram_send_cq[i] =
        ibv_create_cq(cb->ctx, cb->send_q_depth[i], NULL, NULL, 0);
    assert(cb->dgram_send_cq[i] != NULL);

    // <vasilis> I am replacing the recv_cq Depth
    // cb->dgram_recv_cq[i] = ibv_create_cq(cb->ctx,
    // 	HRD_Q_DEPTH, NULL, NULL, 0);
    cb->dgram_recv_cq[i] =
        ibv_create_cq(cb->ctx, cb->recv_q_depth[i], NULL, NULL, 0);
    assert(cb->dgram_recv_cq[i] != NULL);
    // </vasilis>

    /* Initialize creation attributes */
    struct ibv_qp_init_attr create_attr;
    memset((void*)&create_attr, 0, sizeof(struct ibv_qp_init_attr));
    // if (i > 0) printf("The recv queue %d has size %d, the send queue has size
    // 		%d\n", i, cb->recv_q_depth[i], cb->send_q_depth[i] );
    create_attr.send_cq = cb->dgram_send_cq[i];
    create_attr.recv_cq = cb->dgram_recv_cq[i];
    create_attr.qp_type = IBV_QPT_UD;

    create_attr.cap.max_send_wr = cb->send_q_depth[i];
    // <vasilis>
    // printf("Receive q depth %d\n", cb->recv_q_depth);
    create_attr.cap.max_recv_wr = cb->recv_q_depth[i];
    // </vasilis>
    create_attr.cap.max_send_sge = 1;
    create_attr.cap.max_recv_sge = 1;
    create_attr.cap.max_inline_data = HRD_MAX_INLINE;

    cb->dgram_qp[i] = ibv_create_qp(cb->pd, &create_attr);
    assert(cb->dgram_qp[i] != NULL);

    /* INIT state */
    struct ibv_qp_attr init_attr;
    memset((void*)&init_attr, 0, sizeof(struct ibv_qp_attr));
    init_attr.qp_state = IBV_QPS_INIT;
    init_attr.pkey_index = 0;
    init_attr.port_num = cb->dev_port_id;
    init_attr.qkey = HRD_DEFAULT_QKEY;

    if (ibv_modify_qp(
            cb->dgram_qp[i], &init_attr,
            IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY)) {
      fprintf(stderr, "Failed to modify dgram QP to INIT\n");
      return;
    }

    /* RTR state */
    struct ibv_qp_attr rtr_attr;
    memset((void*)&rtr_attr, 0, sizeof(struct ibv_qp_attr));
    rtr_attr.qp_state = IBV_QPS_RTR;

    if (ibv_modify_qp(cb->dgram_qp[i], &rtr_attr, IBV_QP_STATE)) {
      fprintf(stderr, "Failed to modify dgram QP to RTR\n");
      exit(-1);
    }

    /* Reuse rtr_attr for RTS */
    rtr_attr.qp_state = IBV_QPS_RTS;
    rtr_attr.sq_psn = HRD_DEFAULT_PSN;

    if (ibv_modify_qp(cb->dgram_qp[i], &rtr_attr,
                      IBV_QP_STATE | IBV_QP_SQ_PSN)) {
      fprintf(stderr, "Failed to modify dgram QP to RTS\n");
      exit(-1);
    }
  }
}

void
hrd_publish_dgram_qp(struct hrd_ud_ctrl_blk* cb, int n, const char* qp_name,
                     uint8_t sl)
{
  assert(cb != NULL);
  assert(n >= 0 && n < cb->num_dgram_qps);

  assert(qp_name != NULL && strlen(qp_name) < HRD_QP_NAME_SIZE - 1);
  assert(strstr(qp_name, HRD_RESERVED_NAME_PREFIX) == NULL);

  int len = strlen(qp_name);
  int i;
  for (i = 0; i < len; i++) {
    if (qp_name[i] == ' ') {
      fprintf(stderr, "HRD: Space not allowed in QP name\n");
      exit(-1);
    }
  }

  struct hrd_qp_attr qp_attr;
  memcpy(qp_attr.name, qp_name, len);
  qp_attr.name[len] = 0; /* Add the null terminator */
  qp_attr.lid = hrd_get_local_lid(cb->dgram_qp[n]->context, cb->dev_port_id);
  qp_attr.qpn = cb->dgram_qp[n]->qp_num;
  qp_attr.sl = sl;

  // <Vasilis>  ---ROCE----------
  if (is_roce == 1) {
    union ibv_gid ret_gid;
    ibv_query_gid(cb->ctx, IB_PHYS_PORT, 0, &ret_gid);
    qp_attr.gid_global_interface_id = ret_gid.global.interface_id;
    qp_attr.gid_global_subnet_prefix = ret_gid.global.subnet_prefix;
  }
  // printf("Publishing datagram qp with name %s \n", qp_attr.name);
  // </vasilis>

  hrd_publish(qp_attr.name, &qp_attr, sizeof(struct hrd_qp_attr));
}

struct hrd_qp_attr*
hrd_get_published_qp(const char* qp_name)
{
  struct hrd_qp_attr* ret;
  assert(qp_name != NULL && strlen(qp_name) < HRD_QP_NAME_SIZE - 1);
  assert(strstr(qp_name, HRD_RESERVED_NAME_PREFIX) == NULL);

  int len = strlen(qp_name);
  int i;
  for (i = 0; i < len; i++) {
    if (qp_name[i] == ' ') {
      fprintf(stderr, "HRD: Space not allowed in QP name\n");
      exit(-1);
    }
  }

  int ret_len = hrd_get_published(qp_name, (void**)&ret);

  /*
   * The registry lookup returns only if we get a unique QP for @qp_name, or
   * if the memcached lookup succeeds but we don't have an entry for @qp_name.
   */
  assert(ret_len == sizeof(struct hrd_qp_attr) || ret_len == -1);

  return ret;
}

//////////////////////////
/// Fun-c-print
//////////////////////////

/* Like printf, but colorfur. Limited to 1000 characters. */
void
colored_printf(color_print_t color, const char* format, ...)
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
  for (i = RED_LIM - 1; i >= RED_LIM - 50; i--) {
    assert(buf1[i] == 0);
  }

  /* Add markers for red color and reset color */
  // snprintf(buf2, 1000, "\033[31m%s\033[0m", buf1);
  snprintf(buf2, 1000, "\033[31m%s\033[0m", buf1);
  switch (color) {
    case YELLOW:
      snprintf(buf2, 1000, "\033[33m%s\033[0m", buf1);
      break;
    case RED:
      snprintf(buf2, 1000, "\033[31m%s\033[0m", buf1);
      break;
    case GREEN:
      snprintf(buf2, 1000, "\033[32m%s\033[0m", buf1);
      break;
    case CYAN:
      snprintf(buf2, 1000, "\033[36m%s\033[0m", buf1);
      break;
    default:
      printf("Wrong printf color /%d \n", color);
      assert(false);
  }

  /* Probably another bad check for buffer overflow */
  for (i = RED_LIM - 1; i >= RED_LIM - 50; i--) {
    assert(buf2[i] == 0);
  }

  printf("%s", buf2);

  va_end(args);
}
