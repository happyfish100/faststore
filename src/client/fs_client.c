#include "fs_client.h"

int fs_unlink_file(FSClientContext *client_ctx, const int64_t oid,
        const int64_t file_size)
{
    FSBlockKey bkey;
    int64_t remain;
    int result;

    remain = file_size;
    fs_set_block_key(&bkey, oid, 0);
    while (1) {
        logInfo("block {oid: %"PRId64", offset: %"PRId64"}",
                bkey.oid, bkey.offset);

        result = fs_client_proto_block_delete(client_ctx, &bkey);
        if (result == ENOENT) {
            result = 0;
        } else if (result != 0) {
            break;
        }

        remain -= FS_FILE_BLOCK_SIZE;
        if (remain <= 0) {
            break;
        }

        fs_next_block_key(&bkey);
    }

    return result;
}
