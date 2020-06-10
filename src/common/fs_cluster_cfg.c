#include <limits.h>
#include "fastcommon/shared_func.h"
#include "fastcommon/logger.h"
#include "fs_cluster_cfg.h"

#define INIT_ID_ARRAY(array) \
    do {  \
        (array).alloc = (array).count = 0; \
        (array).ids = NULL;  \
    } while (0)

static int compare_server_data_mapping(const void *p1, const void *p2)
{
    return ((FSServerDataMapping *)p1)->server_id -
        ((FSServerDataMapping *)p2)->server_id;
}

static int alloc_server_data_mappings(FSClusterConfig *cluster_cfg)
{
    int bytes;
    FSServerDataMapping *mapping;
    FCServerInfo *server;
    FCServerInfo *end;

    bytes = sizeof(FSServerDataMapping) * FC_SID_SERVER_COUNT(
            cluster_cfg->server_cfg);
    cluster_cfg->server_data_mappings.mappings =
        (FSServerDataMapping *)malloc(bytes);
    if (cluster_cfg->server_data_mappings.mappings == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }
    memset(cluster_cfg->server_data_mappings.mappings, 0, bytes);

    end = FC_SID_SERVERS(cluster_cfg->server_cfg) +
        FC_SID_SERVER_COUNT(cluster_cfg->server_cfg);
    for (server=FC_SID_SERVERS(cluster_cfg->server_cfg),
            mapping=cluster_cfg->server_data_mappings.mappings;
            server<end; server++, mapping++)
    {
        mapping->server_id = server->id;
    }
    cluster_cfg->server_data_mappings.count = FC_SID_SERVER_COUNT(
            cluster_cfg->server_cfg);
    qsort(cluster_cfg->server_data_mappings.mappings,
            cluster_cfg->server_data_mappings.count,
            sizeof(FSServerDataMapping),
            compare_server_data_mapping);
    return 0;
}

static int load_server_group_count(FSClusterConfig *cluster_cfg,
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

static int load_data_group_count(FSClusterConfig *cluster_cfg,
        const char *cluster_filename, IniContext *ini_context)
{
    int data_group_count;
    int bytes;
    FSDataServerMapping *mapping;
    FSDataServerMapping *mend;

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

    mend = cluster_cfg->data_groups.mappings + cluster_cfg->data_groups.count;
    for (mapping=cluster_cfg->data_groups.mappings; mapping<mend; mapping++) {
        mapping->data_group_id = (mapping - cluster_cfg->
            data_groups.mappings) + 1;
    }
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

static int id_array_to_string_ex(FSIdArray *id_array,
        string_t *buff, const int buff_size)
{
    char *p;
    char *end;
    char tmp[32];
    int len;
    int i;

    if (id_array->count == 0) {
        *buff->str = '\0';
        buff->len = 0;
        return 0;
    }

    end = buff->str + buff_size;
    p = buff->str;
    p += sprintf(p, "%d", id_array->ids[0]);
    for (i=1; i<id_array->count; i++) {
        len = sprintf(tmp, ", %d", id_array->ids[i]);
        if (end - p <= len) {
            return ENOSPC;
        }

        memcpy(p, tmp, len);
        p += len;
    }

    *p = '\0';
    buff->len = p - buff->str;
    return 0;
}

static int id_array_to_string(FSIdArray *id_array,
        char *id_str, const int buff_size)
{
    string_t buff;
    buff.str = id_str;
    return id_array_to_string_ex(id_array, &buff, buff_size);
}

static int check_data_groups(FSClusterConfig *cluster_cfg,
        const char *cluster_filename)
{
    FSDataServerMapping *mapping;
    FSDataServerMapping *mend;
    FSIdArray data_group_ids;
    int result;

    INIT_ID_ARRAY(data_group_ids);
    mend = cluster_cfg->data_groups.mappings + cluster_cfg->data_groups.count;
    for (mapping=cluster_cfg->data_groups.mappings; mapping<mend; mapping++) {
        if (mapping->server_group == NULL) {
            if ((result=check_realloc_id_array(&data_group_ids, 1)) != 0) {
                return result;
            }
            data_group_ids.ids[data_group_ids.count++] = mapping->data_group_id;
        }
    }

    if (data_group_ids.ids != NULL) {
        char id_buff[1024];

        id_array_to_string(&data_group_ids, id_buff, sizeof(id_buff));
        logError("file: "__FILE__", line: %d, "
                "config file: %s, %d data group ids: %s NOT belong to "
                "any server group!", __LINE__, cluster_filename,
                data_group_ids.count, id_buff);
        free(data_group_ids.ids);
        return ENOENT;
    }

    return 0;
}

static int check_server_data_mappings(FSClusterConfig *cluster_cfg,
        const char *cluster_filename)
{
    FSIdArray server_ids;
    FSServerDataMapping *mapping;
    FSServerDataMapping *mend;

    INIT_ID_ARRAY(server_ids);
    mend = cluster_cfg->server_data_mappings.mappings +
        cluster_cfg->server_data_mappings.count;
    for (mapping=cluster_cfg->server_data_mappings.mappings;
            mapping<mend; mapping++)
    {
        if (mapping->data_group.count == 0) {
            if (check_realloc_id_array(&server_ids, 1) == 0) {
                server_ids.ids[server_ids.count++] = mapping->server_id;
            }
        }
    }

    if (server_ids.ids != NULL) {
        char id_buff[1024];

        id_array_to_string(&server_ids, id_buff, sizeof(id_buff));
        logWarning("file: "__FILE__", line: %d, "
                "config file: %s, %d server ids: %s NOT used (NOT contain "
                "any data group)!", __LINE__, cluster_filename,
                server_ids.count, id_buff);
        free(server_ids.ids);
    }

    return 0;
}

static int parse_range(const char *cluster_filename, IniContext *ini_context,
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
                sprintf(err_msg, "expect comma char \",\" "
                        "or minus char \"-\"");
                result = EINVAL;
                break;
            }
            p++;
        } else if (*endptr == ',' || *endptr == '-') {
            p = endptr + 1;
        } else {
            sprintf(err_msg, "expect comma char \",\" "
                    "or minus char \"-\"");
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
            sprintf(err_msg, "invalid char: %c (0x%02x), expect rang end "
                    "char \"]\"", *p, *((unsigned char *)p));
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
                "config file: %s, section: %s, item: %s, value: \"%s\", %s",
                __LINE__, cluster_filename, section_name, item_name,
                item_value, err_msg);
        return result;
    }

    if ((result=check_realloc_id_array(id_array,
                    end_id - start_id + 1)) != 0)
    {
        return result;
    }

    for (id=start_id; id<=end_id; id++) {
        id_array->ids[id_array->count++] = id;
    }

    return 0;
}

static int parse_value(const char *cluster_filename, IniContext *ini_context,
        const char *section_name, const char *item_name,
        const char *item_value, FSIdArray *id_array)
{
#define MAX_VALUE_PARTS   64
    char value[FAST_INI_ITEM_VALUE_SIZE];
    char *parts[MAX_VALUE_PARTS];
    char *v;
    char *endptr;
    char err_msg[128];
    int result;
    int count;
    int id;
    int i;

    snprintf(value, sizeof(value), "%s", item_value);
    count = splitEx(value, ',', parts, MAX_VALUE_PARTS);
    if ((result=check_realloc_id_array(id_array, count)) != 0) {
        return result;
    }

    *err_msg = '\0';
    for (i=0; i<count; i++) {
        v = fc_trim(parts[i]);
        if (*v == '\0') {
            if (count > 1) {
                sprintf(err_msg, "the %dth id is empty", i + 1);
            } else {
                sprintf(err_msg, "empty id");
            }
            result = EINVAL;
            break;
        }

        id = strtol(v, &endptr, 10);
        if (*endptr != '\0') {
            sprintf(err_msg, "unexpect char: %c (0x%02x)",
                *endptr, *((unsigned char *)endptr));
            result = EINVAL;
            break;
        }
        if (id <= 0) {
            sprintf(err_msg, "invalid id: %d <= 0", id);
            result = EINVAL;
            break;
        }
        id_array->ids[id_array->count++] = id;
    }

    if (result != 0) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, item: %s, value: \"%s\", %s",
                __LINE__, cluster_filename, section_name, item_name,
                item_value, err_msg);
    }

    return result;
}

static int parse_ids(const char *cluster_filename, IniContext *ini_context,
        const char *section_name, const char *item_name,
        const char *item_value, FSIdArray *id_array)
{
    int value_len;
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
        return parse_range(cluster_filename, ini_context,
                section_name, item_name, item_value, end, id_array);
    } else {
        return parse_value(cluster_filename, ini_context,
                section_name, item_name, item_value, id_array);
    }
}

static int compare_id(const void *p1, const void *p2)
{
    return *((int *)p1) - *((int *)p2);
}

static int get_ids(const char *cluster_filename, IniContext *ini_context,
        const char *section_name, const char *item_name,
        FSIdArray *id_array)
{
    IniItem *items;
    IniItem *it;
    IniItem *end;
    int item_count;
    int result;
    int i;

    if ((items=iniGetValuesEx(section_name, item_name,
                    ini_context, &item_count)) == NULL)
    {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, section: %s, item \"%s\" not exist",
                __LINE__, cluster_filename, section_name, item_name);
        return ENOENT;
    }

    if ((result=check_realloc_id_array(id_array, 4 * item_count)) != 0) {
        return result;
    }

    end = items + item_count;
    for (it=items; it<end; it++) {
        if ((result=parse_ids(cluster_filename, ini_context,
                        section_name, item_name, it->value, id_array)) != 0)
        {
            return result;
        }
    }

    if (id_array->count > 1) {
        qsort(id_array->ids, id_array->count, sizeof(int), compare_id);
        for (i=1; i<id_array->count; i++) {
            if (id_array->ids[i] == id_array->ids[i - 1]) {
                logError("file: "__FILE__", line: %d, "
                        "config file: %s, section: %s, item: %s, "
                        "duplicate id: %d", __LINE__, cluster_filename,
                        section_name, item_name, id_array->ids[i]);
                return EEXIST;
            }
        }
    }

    return 0;
}

static int set_server_group(FSClusterConfig *cluster_cfg,
        const char *cluster_filename, const int server_group_id,
        FSServerGroup *server_group, const FSIdArray *server_ids)
{
    int i;
    int bytes;
    FCServerInfo **pp;

    bytes = sizeof(FCServerInfo *) * server_ids->count;
    server_group->server_array.servers = (FCServerInfo **)malloc(bytes);
    if (server_group->server_array.servers == NULL) {
        logError("file: "__FILE__", line: %d, "
                "malloc %d bytes fail", __LINE__, bytes);
        return ENOMEM;
    }

    pp = server_group->server_array.servers;
    for (i=0; i<server_ids->count; i++) {
        if ((*pp=fc_server_get_by_id(&cluster_cfg->server_cfg,
                        server_ids->ids[i])) == NULL)
        {
            logError("file: "__FILE__", line: %d, "
                    "config file: %s, server group: %d, "
                    "server id: %d not exist", __LINE__,
                    cluster_filename, server_group_id, server_ids->ids[i]);
            return ENOENT;
        }

        pp++;
    }

    server_group->server_array.count = server_ids->count;
    return 0;
}

static int set_data_group(FSClusterConfig *cluster_cfg,
        const char *cluster_filename, const int server_group_id,
        FSServerGroup *server_group)
{
    int result;
    int i;
    int data_group_id;
    FSDataServerMapping *mapping;
    char err_msg[128];

    result = 0;
    *err_msg = '\0';
    for (i=0; i<server_group->data_group.count; i++) {
        data_group_id = server_group->data_group.ids[i];
        if (data_group_id > cluster_cfg->data_groups.count) {
            sprintf(err_msg, "data group id: %d > data group count: %d",
                    data_group_id, cluster_cfg->data_groups.count);
            result = EOVERFLOW;
            break;
        }

        mapping = cluster_cfg->data_groups.mappings + (data_group_id - 1);
        if (mapping->server_group != NULL) {
            sprintf(err_msg, "data group id: %d already exist "
                    "in server group: %d", data_group_id, mapping->
                    server_group->server_group_id);
            result = EEXIST;
            break;
        }

        mapping->server_group = server_group;
    }

    if (result != 0) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, server group id: %d, %s", __LINE__,
                cluster_filename, server_group_id, err_msg);
    }

    return result;
}

static int add_to_id_array(FSIdArray *dest, const FSIdArray *src)
{
    int *pd;
    int *ps;
    int *send;
    int result;

    if ((result=check_realloc_id_array(dest, src->count)) != 0) {
        return result;
    }

    pd = dest->ids + dest->count;
    send = src->ids + src->count;
    for (ps=src->ids; ps<send; ps++) {
        *pd++ = *ps;
    }
    dest->count += src->count;
    if (dest->count > 1) {
        qsort(dest->ids, dest->count, sizeof(int), compare_id);
    }

    return 0;
}

static int set_server_data_mappings(FSClusterConfig *cluster_cfg,
        const char *cluster_filename, const int server_group_id,
        FSServerGroup *server_group)
{
    FCServerInfo **pp;
    FCServerInfo **end;
    FSServerDataMapping target;
    FSServerDataMapping *mapping;
    int result;

    end = server_group->server_array.servers +
        server_group->server_array.count;
    for (pp=server_group->server_array.servers; pp<end; pp++) {
        target.server_id = (*pp)->id;
        mapping = (FSServerDataMapping *)bsearch(&target,
                cluster_cfg->server_data_mappings.mappings,
                cluster_cfg->server_data_mappings.count,
                sizeof(FSServerDataMapping),
                compare_server_data_mapping);
        if (mapping == NULL) {
            logError("file: "__FILE__", line: %d, "
                    "config file: %s, server group id: %d, "
                    "can't found server id: %d", __LINE__,
                    cluster_filename, server_group_id, (*pp)->id);
            return ENOENT;
        }

        if ((result=add_to_id_array(&mapping->data_group,
                        &server_group->data_group)) != 0)
        {
            return result;
        }

        if (mapping->data_group.count > FS_MAX_DATA_GROUPS_PER_SERVER) {
            logError("file: "__FILE__", line: %d, "
                    "config file: %s, server group id: %d, "
                    "server id: %d, too many group data ids: %d "
                    "exceeds %d", __LINE__, cluster_filename,
                    server_group_id, (*pp)->id, mapping->data_group.count,
                    FS_MAX_DATA_GROUPS_PER_SERVER);
            return EOVERFLOW;
        }
    }

    return 0;
}

static int load_one_server_group(FSClusterConfig *cluster_cfg,
        const char *cluster_filename, IniContext *ini_context,
        const int server_group_id, FSServerGroup *server_group,
        FSIdArray *server_ids)
{
    char section_name[32];
    int result;

    server_group->server_group_id = server_group_id;
    sprintf(section_name, "server-group-%d", server_group_id);

    server_ids->count = 0;
    if ((result=get_ids(cluster_filename, ini_context,
                    section_name, "server_ids", server_ids)) != 0)
    {
        return result;
    }

    if ((result=get_ids(cluster_filename, ini_context,
                    section_name, "data_group_ids", &server_group->
                    data_group)) != 0)
    {
        return result;
    }

    if (server_group->data_group.count > FS_MAX_DATA_GROUPS_PER_SERVER) {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, server group id: %d, "
                "too many group data ids: %d exceeds %d",
                __LINE__, cluster_filename, server_group_id,
                server_group->data_group.count,
                FS_MAX_DATA_GROUPS_PER_SERVER);
        return EOVERFLOW;
    }

    if ((result=set_server_group(cluster_cfg, cluster_filename,
                    server_group_id, server_group, server_ids)) != 0)
    {
        return result;
    }

    if ((result=set_data_group(cluster_cfg, cluster_filename,
                    server_group_id, server_group)) != 0)
    {
        return result;
    }

    return set_server_data_mappings(cluster_cfg, cluster_filename,
            server_group_id, server_group);
}

static int load_groups(FSClusterConfig *cluster_cfg,
        const char *cluster_filename, IniContext *ini_context)
{
    int result;
    int i;
    int server_group_id;
    FSIdArray server_ids;

    if ((result=load_server_group_count(cluster_cfg, cluster_filename,
                    ini_context)) != 0)
    {
        return result;
    }

    if ((result=load_data_group_count(cluster_cfg, cluster_filename,
                    ini_context)) != 0)
    {
        return result;
    }

    INIT_ID_ARRAY(server_ids);
    for (i=0; i<cluster_cfg->server_groups.count; i++) {
        server_group_id = i + 1;
        if ((result=load_one_server_group(cluster_cfg, cluster_filename,
                        ini_context, server_group_id, cluster_cfg->
                        server_groups.groups + i, &server_ids)) != 0)
        {
            return result;
        }
    }

    if (server_ids.ids != NULL) {
        free(server_ids.ids);
    }

    if ((result=check_data_groups(cluster_cfg, cluster_filename)) != 0) {
        return result;
    }
    return check_server_data_mappings(cluster_cfg, cluster_filename);
}

static int find_group_indexes_in_cluster_config(FSClusterConfig *cluster_cfg,
        const char *filename)
{
    cluster_cfg->cluster_group_index = fc_server_get_group_index(
            &cluster_cfg->server_cfg, "cluster");
    if (cluster_cfg->cluster_group_index < 0) {
        logError("file: "__FILE__", line: %d, "
                "cluster config file: %s, cluster group not configurated",
                __LINE__, filename);
        return ENOENT;
    }

    cluster_cfg->service_group_index = fc_server_get_group_index(
            &cluster_cfg->server_cfg, "service");
    if (cluster_cfg->service_group_index < 0) {
        logError("file: "__FILE__", line: %d, "
                "cluster config file: %s, service group not configurated",
                __LINE__, filename);
        return ENOENT;
    }

    return 0;
}

int fs_cluster_cfg_load(FSClusterConfig *cluster_cfg,
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

    if ((result=alloc_server_data_mappings(cluster_cfg)) != 0) {
        return result;
    }

    result = load_groups(cluster_cfg, cluster_filename, &ini_context);
    iniFreeContext(&ini_context);
    if (result != 0) {
        return result;
    }

    return find_group_indexes_in_cluster_config(
            cluster_cfg, cluster_filename);
}

void fs_cluster_cfg_destroy(FSClusterConfig *cluster_cfg)
{
    FSServerGroup *sgroup;
    FSServerGroup *send;

    if (cluster_cfg->server_groups.groups == NULL) {
        return;
    }

    send = cluster_cfg->server_groups.groups + cluster_cfg->server_groups.count;
    for (sgroup=cluster_cfg->server_groups.groups; sgroup<send; sgroup++) {
        if (sgroup->server_array.servers != NULL) {
            free(sgroup->server_array.servers);
            sgroup->server_array.servers = NULL;
        }
        if (sgroup->data_group.ids != NULL) {
            free(sgroup->data_group.ids);
            INIT_ID_ARRAY(sgroup->data_group);
        }
    }
    free(cluster_cfg->server_groups.groups);
    cluster_cfg->server_groups.groups = NULL;

    if (cluster_cfg->data_groups.mappings != NULL) {
        free(cluster_cfg->data_groups.mappings);
        cluster_cfg->data_groups.mappings = NULL;
    }

    if (cluster_cfg->server_data_mappings.mappings != NULL) {
        FSServerDataMapping *mapping;
        FSServerDataMapping *mend;

        mend = cluster_cfg->server_data_mappings.mappings +
            cluster_cfg->server_data_mappings.count;
        for (mapping=cluster_cfg->server_data_mappings.mappings;
                mapping<mend; mapping++)
        {
            free(mapping->data_group.ids);
            INIT_ID_ARRAY(mapping->data_group);
        }
        free(cluster_cfg->server_data_mappings.mappings);
        cluster_cfg->server_data_mappings.mappings = NULL;
    }
}

void fs_cluster_cfg_to_log(FSClusterConfig *cluster_cfg)
{
    FSServerGroup *sgroup;
    FSServerGroup *send;
    char server_id_buff[1024];
    char group_id_buff[1024];
    char *p;
    char *buff_end;
    int i;

    logInfo("server_group_count = %d", cluster_cfg->server_groups.count);
    logInfo("data_group_count = %d", cluster_cfg->data_groups.count);

    buff_end = server_id_buff + sizeof(server_id_buff);
    send = cluster_cfg->server_groups.groups +
        cluster_cfg->server_groups.count;
    for (sgroup=cluster_cfg->server_groups.groups; sgroup<send; sgroup++) {
        p = server_id_buff;
        p += sprintf(p, "%d", sgroup->server_array.servers[0]->id);
        for (i=1; i<sgroup->server_array.count; i++) {
            p += snprintf(p, buff_end - p, ", %d",
                    sgroup->server_array.servers[i]->id);
        }

        id_array_to_string(&sgroup->data_group, group_id_buff,
                sizeof(group_id_buff));

        logInfo("[server-group-%d]", sgroup->server_group_id);
        logInfo("server_ids = %s", server_id_buff);
        logInfo("data_group_ids = %s", group_id_buff);
    }
}

int fc_cluster_cfg_to_string(FSClusterConfig *cluster_cfg, FastBuffer *buffer)
{
    int result;
    FSServerGroup *sgroup;
    FSServerGroup *send;
    char server_id_buff[1024];
    char group_id_buff[1024];
    char tmp[32];
    string_t sid_buff;
    string_t gid_buff;
    char *p;
    char *buff_end;
    int len;
    int i;

    if ((result=fast_buffer_check(buffer, cluster_cfg->server_groups.count *
                    256)) != 0)
    {
        return result;
    }

    if ((result=fast_buffer_append(buffer,
                    "server_group_count = %d\n"
                    "data_group_count = %d\n",
                    cluster_cfg->server_groups.count,
                    cluster_cfg->data_groups.count)) != 0)
    {
        return result;
    }

    sid_buff.str = server_id_buff;
    gid_buff.str = group_id_buff;
    buff_end = server_id_buff + sizeof(server_id_buff);
    send = cluster_cfg->server_groups.groups +
        cluster_cfg->server_groups.count;
    for (sgroup=cluster_cfg->server_groups.groups; sgroup<send; sgroup++) {
        p = sid_buff.str;
        p += sprintf(p, "%d", sgroup->server_array.servers[0]->id);
        for (i=1; i<sgroup->server_array.count; i++) {
            len = sprintf(tmp, ", %d", sgroup->server_array.servers[i]->id);
            if (buff_end - p <= len) {
                return ENOSPC;
            }

            memcpy(p, tmp, len);
            p += len;
        }
        *p = '\0';
        sid_buff.len = p - sid_buff.str;

        if ((result=id_array_to_string_ex(&sgroup->data_group, &gid_buff,
                        sizeof(group_id_buff))) != 0)
        {
            return result;
        }

        if ((result=fast_buffer_append(buffer,
                        "[server-group-%d]\n"
                        "server_ids = %s\n"
                        "data_group_ids = %s\n",
                        sgroup->server_group_id,
                        sid_buff.str, gid_buff.str)) != 0)
        {
            return result;
        }
    }

    return 0;
}

static int compare_server_group(const void *p1, const void *p2)
{
    return (*((FSServerGroup **)p1))->server_group_id -
        (*((FSServerGroup **)p2))->server_group_id;
}

int fs_cluster_cfg_add_one_server(FCServerInfo *svr,
        FCServerInfo **servers, const int size, int *count)
{
    FCServerInfo **pp;
    FCServerInfo **current;
    FCServerInfo **end;

    end = servers + *count;
    for (pp=servers; pp<end; pp++) {
        if ((*pp)->id == svr->id) {
            return 0;
        } else if ((*pp)->id > svr->id) {
            break;
        }
    }

    if (size < (*count) + 1) {
        return ENOSPC;
    }

    current = pp;
    for (pp=end; pp>current; pp--) {
        *pp = *(pp - 1);
    }

    *current = svr;
    (*count)++;
    return 0;
}

int fs_cluster_cfg_add_servers(FSServerGroup *server_group,
        FCServerInfo **servers, const int size, int *count)
{
    FCServerInfo **pp;
    FCServerInfo **end;
    int result;

    end = server_group->server_array.servers + server_group->server_array.count;
    for (pp=server_group->server_array.servers; pp<end; pp++) {
        if ((result=fs_cluster_cfg_add_one_server(*pp,
                        servers, size, count)) != 0)
        {
            return result;
        }
    }

    return 0;
}

int fs_cluster_cfg_get_group_servers(FSClusterConfig *cluster_cfg,
        const int server_id, FCServerInfo **servers,
        const int size, int *count)
{
    FSServerDataMapping target;
    FSServerDataMapping *mapping;
    FSServerGroup *server_groups[FS_MAX_DATA_GROUPS_PER_SERVER];
    int result;
    int i;
    int data_group_index;
    int sgroup_count;

    *count = 0;
    target.server_id = server_id;
    mapping = (FSServerDataMapping *)bsearch(&target,
            cluster_cfg->server_data_mappings.mappings,
            cluster_cfg->server_data_mappings.count,
            sizeof(FSServerDataMapping),
            compare_server_data_mapping);
    if (mapping == NULL) {
        return ENOENT;
    }

    sgroup_count = 0;
    for (i=0; i<mapping->data_group.count; i++) {
        data_group_index = mapping->data_group.ids[i] - 1;

        server_groups[sgroup_count++] = cluster_cfg->data_groups.
            mappings[data_group_index].server_group;
    }
    if (sgroup_count == 0) {
        return ENOENT;
    }

    qsort(server_groups, sgroup_count, sizeof(FSServerGroup *),
            compare_server_group);

    if ((result=fs_cluster_cfg_add_servers(server_groups[0],
                    servers, size, count)) != 0)
    {
        return result;
    }
    for (i=1; i<sgroup_count; i++) {
        if (server_groups[i] != server_groups[i -1]) {
            if ((result=fs_cluster_cfg_add_servers(server_groups[i],
                            servers, size, count)) != 0)
            {
                return result;
            }
        }
    }

    return 0;
}

FSIdArray *fs_cluster_cfg_get_server_group_ids(FSClusterConfig *cluster_cfg,
        const int server_id)
{
    FSServerDataMapping target;
    FSServerDataMapping *mapping;

    target.server_id = server_id;
    mapping = (FSServerDataMapping *)bsearch(&target,
            cluster_cfg->server_data_mappings.mappings,
            cluster_cfg->server_data_mappings.count,
            sizeof(FSServerDataMapping),
            compare_server_data_mapping);
    return (mapping != NULL) ?  &mapping->data_group : NULL;
}

int fs_cluster_cfg_get_server_max_group_id(FSClusterConfig *cluster_cfg,
        const int server_id)
{
    FSServerDataMapping target;
    FSServerDataMapping *mapping;

    target.server_id = server_id;
    mapping = (FSServerDataMapping *)bsearch(&target,
            cluster_cfg->server_data_mappings.mappings,
            cluster_cfg->server_data_mappings.count,
            sizeof(FSServerDataMapping),
            compare_server_data_mapping);
    if (mapping != NULL && mapping->data_group.count > 0) {
        return mapping->data_group.ids[mapping->data_group.count - 1];
    } else {
        return -1;
    }
}

int fs_cluster_cfg_load_from_ini(FSClusterConfig *cluster_cfg,
        IniContext *ini_context, const char *cfg_filename)
{
    char *cluster_cfg_filename;
    char cluster_full_filename[PATH_MAX];

    cluster_cfg_filename = iniGetStrValue(NULL,
            "cluster_config_filename", ini_context);
    if (cluster_cfg_filename == NULL || *cluster_cfg_filename == '\0') {
        logError("file: "__FILE__", line: %d, "
                "config file: %s, item \"cluster_config_filename\" "
                "not exist or empty", __LINE__, cfg_filename);
        return ENOENT;
    }

    resolve_path(cfg_filename, cluster_cfg_filename,
            cluster_full_filename, sizeof(cluster_full_filename));
    return fs_cluster_cfg_load(cluster_cfg, cluster_full_filename);
}
