
#ifndef _FS_SERVER_FUNC_H
#define _FS_SERVER_FUNC_H

#include "server_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int server_load_config(const char *filename);

#define server_expect_body_length(task, expect_body_len) \
    sf_server_expect_body_length(&RESPONSE, REQUEST.header.body_len, \
            expect_body_len)

#define server_check_min_body_length(task, min_body_length) \
    sf_server_check_min_body_length(&RESPONSE, REQUEST.header.body_len, \
            min_body_length)

#define server_check_max_body_length(task, max_body_length) \
    sf_server_check_max_body_length(&RESPONSE, REQUEST.header.body_len, \
            max_body_length)

#define server_check_body_length(task, min_body_length, max_body_length) \
    sf_server_check_body_length(&RESPONSE, REQUEST.header.body_len, \
            min_body_length, max_body_length)

#ifdef __cplusplus
}
#endif

#endif
