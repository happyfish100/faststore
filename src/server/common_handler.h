//common_handler.h

#ifndef FS_COMMON_HANDLER_H
#define FS_COMMON_HANDLER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fastcommon/fast_task_queue.h"
#include "server_types.h"

#ifdef __cplusplus

extern "C" {
#endif

static inline int handler_deal_actvie_test(struct fast_task_info *task)
{
    return server_expect_body_length(task, 0);
}

static inline void handler_init_task_context(struct fast_task_info *task)
{
    TASK_ARG->req_start_time = get_current_time_us();
    RESPONSE.header.cmd = FS_PROTO_ACK;
    RESPONSE.header.body_len = 0;
    RESPONSE.header.status = 0;
    RESPONSE.error.length = 0;
    RESPONSE.error.message[0] = '\0';
    TASK_ARG->context.log_error = true;
    TASK_ARG->context.response_done = false;
    TASK_ARG->context.need_response = true;

    REQUEST.header.cmd = ((FSProtoHeader *)task->data)->cmd;
    REQUEST.header.body_len = task->length - sizeof(FSProtoHeader);
    REQUEST.header.status = buff2short(((FSProtoHeader *)task->data)->status);
    REQUEST.body = task->data + sizeof(FSProtoHeader);
}

int handler_check_config_signs(struct fast_task_info *task,
        const int server_id, FSProtoConfigSigns *config_signs);

int handler_deal_ack(struct fast_task_info *task);

int handler_deal_task_done(struct fast_task_info *task);

#ifdef __cplusplus
}
#endif

#endif
