#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include "fs_fuse_wrapper.h"

int main(int argc, char *argv[])
{
    const char *fdir_config_filename = "/etc/fdir/client.conf";
    const char *fs_config_filename = "/etc/fstore/client.conf";
    const char *ns = "fs";
	struct fuse_lowlevel_ops fuse_operations;
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	struct fuse_session *se;
	struct fuse_cmdline_opts opts;
	int result = -1;

	if (fuse_parse_cmdline(&args, &opts) != 0)
		return 1;
	if (opts.show_help) {
		printf("usage: %s [options] <mountpoint>\n\n", argv[0]);
		fuse_cmdline_help();
		fuse_lowlevel_help();
		result = 0;
		goto err_out1;
	} else if (opts.show_version) {
		printf("FUSE library version %s\n", fuse_pkgversion());
		fuse_lowlevel_version();
		result = 0;
		goto err_out1;
	}

	if(opts.mountpoint == NULL) {
		printf("usage: %s [options] <mountpoint>\n", argv[0]);
		printf("       %s --help\n", argv[0]);
		result = 1;
		goto err_out1;
	}

    log_init2();
    if ((result=fs_api_pooled_init(ns, fdir_config_filename,
                    fs_config_filename)) != 0)
    {
        return result;
    }

	fs_fuse_wrapper_get_ops(&fuse_operations);
	se = fuse_session_new(&args, &fuse_operations,
			      sizeof(fuse_operations), NULL);
	if (se == NULL)
	    goto err_out1;

	if (fuse_set_signal_handlers(se) != 0)
	    goto err_out2;

	if (fuse_session_mount(se, opts.mountpoint) != 0)
	    goto err_out3;

	//fuse_daemonize(opts.foreground);

	/* Block until ctrl+c or fusermount -u */
	if (opts.singlethread)
		result = fuse_session_loop(se);
	else
		result = fuse_session_loop_mt(se, opts.clone_fd);

	fuse_session_unmount(se);
err_out3:
	fuse_remove_signal_handlers(se);
err_out2:
	//fuse_session_destroy(se);
err_out1:
	free(opts.mountpoint);
	fuse_opt_free_args(&args);

	return result ? 1 : 0;
}
