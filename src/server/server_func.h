
#ifndef _FS_SERVER_FUNC_H
#define _FS_SERVER_FUNC_H

#include "server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int server_load_config(const char *filename);

static inline int server_expect_body_length(
        struct fast_task_info *task,
        const int expect_body_length)
{
    if (REQUEST.header.body_len != expect_body_length) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "request body length: %d != %d",
                REQUEST.header.body_len, expect_body_length);
        return EINVAL;
    }

    return 0;
}

static inline int server_check_min_body_length(
        struct fast_task_info *task,
        const int min_body_length)
{
    if (REQUEST.header.body_len < min_body_length) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "request body length: %d < %d",
                REQUEST.header.body_len, min_body_length);
        return EINVAL;
    }

    return 0;
}

static inline int server_check_max_body_length(
        struct fast_task_info *task,
        const int max_body_length)
{
    if (REQUEST.header.body_len > max_body_length) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "request body length: %d > %d",
                REQUEST.header.body_len, max_body_length);
        return EINVAL;
    }

    return 0;
}

static inline int server_check_body_length(
        struct fast_task_info *task,
        const int min_body_length,
        const int max_body_length)
{
    int result;
    if ((result=server_check_min_body_length(task, min_body_length)) != 0) {
        return result;
    }
    return server_check_max_body_length(task, max_body_length);
}


#ifdef __cplusplus
}
#endif

#endif
