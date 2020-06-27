//server_binlog.h

#ifndef _SERVER_BINLOG_H_
#define _SERVER_BINLOG_H_

#include "binlog/binlog_types.h"
#include "binlog/binlog_writer.h"
#include "binlog/trunk_binlog.h"
#include "binlog/slice_binlog.h"
#include "binlog/replica_binlog.h"
#include "replication/binlog_replication.h"
#include "replication/binlog_local_consumer.h"

#ifdef __cplusplus
extern "C" {
#endif

int server_binlog_init();
void server_binlog_destroy();
void server_binlog_terminate();

#ifdef __cplusplus
}
#endif

#endif
