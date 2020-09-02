
#ifndef _CHANNEL_INFO_H
#define _CHANNEL_INFO_H

#include "fastcommon/fast_timer.h"
#include "../../common/fs_types.h"

typedef struct fs_channel_info {
    FastTimerEntry timer;  //must be the first
    uint32_t id;
    struct fs_channel_info *next;
} FSChannelInfo;

#ifdef __cplusplus
extern "C" {
#endif

    int channel_info_init(const uint32_t max_channel_id,
            const uint32_t reserve_interval);

    FSChannelInfo *channel_info_alloc(const uint32_t channel_id);

    void channel_info_release(FSChannelInfo *channel);

    void channel_info_free(FSChannelInfo *channel);

#ifdef __cplusplus
}
#endif

#endif
