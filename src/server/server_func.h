
#ifndef _FS_SERVER_FUNC_H
#define _FS_SERVER_FUNC_H

#include "server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int server_load_config(const char *filename);

#define server_expect_body_length(task, expect_body_len) \
    server_expect_body_length_ex(task, REQUEST.header.body_len, \
            expect_body_len)

#define server_check_min_body_length(task, min_body_length) \
    server_check_min_body_length_ex(task, REQUEST.header.body_len, \
            min_body_length)

#define server_check_max_body_length(task, max_body_length) \
    server_check_max_body_length_ex(task, REQUEST.header.body_len, \
            max_body_length)

#define server_check_body_length(task, min_body_length, max_body_length) \
    server_check_body_length_ex(task, REQUEST.header.body_len, \
            min_body_length, max_body_length)

static inline int server_expect_body_length_ex(struct fast_task_info *task,
        const int body_length, const int expect_body_len)
{
    if (body_length != expect_body_len) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "request body length: %d != %d",
                body_length, expect_body_len);
        return EINVAL;
    }

    return 0;
}

static inline int server_check_min_body_length_ex(struct fast_task_info *task,
        const int body_length, const int min_body_length)
{
    if (body_length < min_body_length) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "request body length: %d < %d",
                body_length, min_body_length);
        return EINVAL;
    }

    return 0;
}

static inline int server_check_max_body_length_ex(struct fast_task_info *task,
        const int body_length, const int max_body_length)
{
    if (body_length > max_body_length) {
        RESPONSE.error.length = sprintf(
                RESPONSE.error.message,
                "request body length: %d > %d",
                body_length, max_body_length);
        return EINVAL;
    }

    return 0;
}

static inline int server_check_body_length_ex(
        struct fast_task_info *task, const int body_length,
        const int min_body_length, const int max_body_length)
{
    int result;
    if ((result=server_check_min_body_length_ex(task,
                    body_length, min_body_length)) != 0)
    {
        return result;
    }
    return server_check_max_body_length_ex(task,
            body_length, max_body_length);
}

#ifdef __cplusplus
}
#endif

#endif
