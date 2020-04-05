#include <limits.h>
#include "fastcommon/ini_file_reader.h"
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fs_cluster_cfg.h"


static int load_server_groups(FSClusterConfig *cluster_cfg,
        const char *cluster_filename, IniContext *ini_context)
{
    int server_group_count;
    int bytes;

    server_group_count = iniGetIntValue(NULL, "server_group_count",
            ini_context, 0);
    if (server_group_count <= 0) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, item \"server_group_count\" "
                "not exist or is invalid", __LINE__, cluster_filename);
        return EINVAL;
    }

    bytes = sizeof(FSServerGroup) * server_group_count;
    cluster_cfg->server_groups.groups = (FSServerGroup *)malloc(bytes);
    if (cluster_cfg->server_groups.groups == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(cluster_cfg->server_groups.groups, 0, bytes);
    cluster_cfg->server_groups.count = server_group_count;
    return 0;
}

static int load_data_groups(FSClusterConfig *cluster_cfg,
        const char *cluster_filename, IniContext *ini_context)
{
    int data_group_count;
    int bytes;

    data_group_count = iniGetIntValue(NULL, "data_group_count",
            ini_context, 0);
    if (data_group_count <= 0) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, item \"data_group_count\" "
                "not exist or is invalid", __LINE__, cluster_filename);
        return EINVAL;
    }

    bytes = sizeof(FSDataServerMapping) * data_group_count;
    cluster_cfg->data_groups.mappings = (FSDataServerMapping *)malloc(bytes);
    if (cluster_cfg->data_groups.mappings == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(cluster_cfg->data_groups.mappings, 0, bytes);
    cluster_cfg->data_groups.count = data_group_count;
    return 0;
}

#define SKIP_SPACE_CHARS(p, end) \
    do {  \
        while (p < end && (*p == ' ' || *p == '\t')) {  \
            p++;   \
        }  \
    } while (0)

static int check_realloc_id_array(FSIdArray *id_array, const int inc)
{
    int target_count;
    int alloc_count;
    int bytes;
    int *ids;

    target_count = id_array->count + inc;
    if (id_array->alloc >= target_count) {
        return 0;
    }

    alloc_count = id_array->alloc == 0 ? 4 : 2 * id_array->alloc;
    while (alloc_count < target_count) {
        alloc_count *= 2;
    }

    bytes = sizeof(int) * alloc_count;
    ids = (int *)malloc(bytes);
    if (ids == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }

    if (id_array->ids != NULL) {
        if (id_array->count > 0) {
            memcpy(ids, id_array->ids, sizeof(int) * id_array->count);
        }
        free(id_array->ids);
    }

    id_array->ids = ids;
    id_array->alloc = alloc_count;
    return 0;
}

static int parse_range(FSClusterConfig *cluster_cfg,
        const char *cluster_filename, IniContext *ini_context,
        const char *section_name, const char *item_name,
        const char *item_value, const char *end, FSIdArray *id_array)
{
    const char *p;
    char *endptr;
    char err_msg[128];
    int start_id;
    int end_id;
    int id;
    int result;

    start_id = end_id = 0;
    do {
        if (*(end - 1) != ']') {
            sprintf(err_msg, "expect rang end char \"]\"");
            result = EINVAL;
            break;
        }

        p = item_value + 1;
        SKIP_SPACE_CHARS(p, end);

        start_id = strtol(p, &endptr, 10);
        if (*endptr == ' ' || *endptr == '\t') {
            p = endptr + 1;
            SKIP_SPACE_CHARS(p, end);
            if (!(*p == ',' || *p == '-')) {
                sprintf(err_msg, "expect comma char \",\"");
                result = EINVAL;
                break;
            }
            p++;
        } else if (*endptr == ',' || *endptr == '-') {
            p = endptr + 1;
        } else {
            sprintf(err_msg, "expect comma char \",\"");
            result = EINVAL;
            break;
        }

        if (start_id <= 0) {
            sprintf(err_msg, "invalid start id: %d", start_id);
            result = EINVAL;
            break;
        }

        SKIP_SPACE_CHARS(p, end);
        end_id = strtol(p, &endptr, 10);

        p = endptr;
        SKIP_SPACE_CHARS(p, end);
        if (*p != ']') {
            sprintf(err_msg, "invalid char: 0x%02x, expect rang end "
                    "char \"]\"", *((unsigned char *)p));
            result = EINVAL;
            break;
        }
        if (start_id > end_id) {
            sprintf(err_msg, "start id: %d > end id: %d", start_id, end_id);
            result = EINVAL;
            break;
        }

        result = 0;
        *err_msg = '\0';
    } while (0);

    if (result != 0) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, item: %s, value: %s, %s",
                __LINE__, cluster_filename, section_name, item_name,
                item_value, err_msg);
    }

    if ((result=check_realloc_id_array(id_array,
                    end_id - start_id + 1)) != 0)
    {
        return result;
    }

    for (id=start_id; id<=end_id; id++) {
        id_array->ids[id_array->count++] = id;
    }

    return result;
}

static int parse_ids(FSClusterConfig *cluster_cfg,
        const char *cluster_filename, IniContext *ini_context,
        const char *section_name, const char *item_name,
        const char *item_value, FSIdArray *id_array)
{
    int value_len;
    //const char *p;
    const char *end;

    value_len = strlen(item_value);
    if (value_len == 0) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, item: %s, empty value",
                __LINE__, cluster_filename, section_name, item_name);
        return EINVAL;
    }

    end = item_value + value_len;
    if (*item_value == '[') {
        return parse_range(cluster_cfg, cluster_filename, ini_context,
                section_name, item_name, item_value, end, id_array);
    }

    //TODO
    return 0;
}

static int get_ids(FSClusterConfig *cluster_cfg,
        const char *cluster_filename, IniContext *ini_context,
        const char *section_name, const char *item_name,
        FSIdArray *id_array)
{
    IniItem *items;
    IniItem *it;
    IniItem *end;
    int item_count;
    int result;

    if ((items=iniGetValuesEx(section_name, item_name,
                    ini_context, &item_count)) == NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, item \"%s\" not exist",
                __LINE__, cluster_filename, section_name, item_name);
        return ENOENT;
    }

    end = items + item_count;
    for (it=items; it<end; it++) {
        if ((result=parse_ids(cluster_cfg, cluster_filename, ini_context,
                        section_name, item_name, it->value, id_array)) != 0)
        {
            return result;
        }
    }

    return 0;
}

#define INIT_ID_ARRAY(array) \
    do {  \
        (array).alloc = (array).count = 0; \
        (array).ids = NULL;  \
    } while (0)

static int load_one_server_group(FSClusterConfig *cluster_cfg,
        const char *cluster_filename, IniContext *ini_context,
        const int server_group_id, FSIdArray *server_ids,
        FSIdArray *data_group_ids)
{
    char section_name[32];
    int result;

    sprintf(section_name, "server-group-%d", server_group_id);

    server_ids->count = 0;
    if ((result=get_ids(cluster_cfg, cluster_filename, ini_context,
                    section_name, "server_ids", server_ids)) != 0)
    {
        return result;
    }

    data_group_ids->count = 0;
    if ((result=get_ids(cluster_cfg, cluster_filename, ini_context,
                    section_name, "data_group_ids", data_group_ids)) != 0)
    {
        return result;
    }

    //TODO

    return 0;
}

static int load_groups(FSClusterConfig *cluster_cfg,
        const char *cluster_filename, IniContext *ini_context)
{
    int result;
    int server_group_id;
    FSIdArray server_ids;
    FSIdArray data_group_ids;

    if ((result=load_server_groups(cluster_cfg, cluster_filename,
                    ini_context)) != 0)
    {
        return result;
    }

    if ((result=load_data_groups(cluster_cfg, cluster_filename,
                    ini_context)) != 0)
    {
        return result;
    }

    INIT_ID_ARRAY(server_ids);
    INIT_ID_ARRAY(data_group_ids);
    for (server_group_id=1; server_group_id<=cluster_cfg->server_groups.count;
            server_group_id++)
    {
        if ((result=load_one_server_group(cluster_cfg, cluster_filename,
                        ini_context, server_group_id, &server_ids,
                        &data_group_ids)) != 0)
        {
            return result;
        }
    }

    //TODO check

    return 0;
}

int fs_cluster_config_load(FSClusterConfig *cluster_cfg,
        const char *cluster_filename)
{
    IniContext ini_context;
    char full_filename[PATH_MAX];
    char *server_config_filename;
    const int min_hosts_each_group = 1;
    const bool share_between_groups = true;
    int result;

    memset(cluster_cfg, 0, sizeof(FSClusterConfig));
    if ((result=iniLoadFromFile(cluster_filename, &ini_context)) != 0) {
        logError("file: "__FILE__", line: %d, "
                "load conf file \"%s\" fail, ret code: %d",
                __LINE__, cluster_filename, result);
        return result;
    }

    server_config_filename = iniGetStrValue(NULL,
            "server_config_filename", &ini_context);
    if (server_config_filename == NULL || *server_config_filename == '\0') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, item \"server_config_filename\" "
                "not exist or empty", __LINE__, cluster_filename);
        return ENOENT;
    }

    resolve_path(cluster_filename, server_config_filename,
            full_filename, sizeof(full_filename));
    if ((result=fc_server_load_from_file_ex(&cluster_cfg->server_cfg,
                    full_filename, FS_SERVER_DEFAULT_CLUSTER_PORT,
                    min_hosts_each_group, share_between_groups)) != 0)
    {
        return result;
    }

    result = load_groups(cluster_cfg, cluster_filename, &ini_context);
    iniFreeContext(&ini_context);
    return result;
}

void fs_cluster_config_destroy(FSClusterConfig *cluster_cfg)
{
}
