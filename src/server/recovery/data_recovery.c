#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <pthread.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "sf/sf_global.h"
#include "../server_global.h"
#include "data_recovery.h"

int data_recovery_init(DataRecoveryContext *ctx, const int data_group_id)
{
    ctx->start_time = get_current_time_ms();
    ctx->data_group_id = data_group_id;
    return 0;
}

void data_recovery_destroy(DataRecoveryContext *ctx)
{
}
