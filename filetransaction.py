#!/usr/bin/python

import shutil
import tempfile

OP_READ = 1
OP_COPY = 2
OP_TRUNC = 4


class FileTransactionException(BaseException):
    pass


class FileTransaction(object):
    " FileTransaction "

    import os
    import errno
    import logging

    def __init__(self):
        self.files = {}
        self.dirs = set()

    def mkdir(self, path, mode=0o777):

        self.os.mkdir(path, mode)
        realpath = self.os.path.realpath(path)
        self.dirs.add(realpath)

    def open(self, file, mode='r', *args, **kwargs):
        " open file and add to transaction set "

        realfile = self.os.path.realpath(file)

        if realfile not in self.files:
            self.files[realfile] = {'fp': []}

        if 'tempfile' in self.files[realfile]:
            fp = open(self.files[realfile]['tempfile'], mode, *args, **kwargs)
            self.files[realfile]['fp'].append(fp)
            return fp

        op_mode = self._get_op_mode(mode)

        if op_mode == OP_READ:
            fp = open(realfile, mode, *args, **kwargs)
            self.files[realfile]['fp'].append(fp)
            return fp

        elif op_mode == OP_COPY:
            (tempfile, stat) = self.open_copy(realfile)

        elif op_mode == OP_TRUNC:
            (tempfile, stat) = self.open_trunc(realfile)

        fp = open(tempfile, mode, *args, **kwargs)

        self.files[realfile]['fp'].append(fp)
        self.files[realfile]['tempfile'] = tempfile
        self.files[realfile]['stat'] = stat
        return fp

    def _get_op_mode(self, mode):
        " return op_mode "

        mode = self._norm_mode(mode)
        if mode == 'r':
            return OP_READ
        elif mode in ('r+', 'a', 'a+'):
            return OP_COPY
        elif mode in ('w', 'w+'):
            return OP_TRUNC
        raise ValueError("incorrect mode")

    def _norm_mode(self, mode):
        " normalize mode "

        ret = mode[0]
        if '+' in mode:
            ret += '+'
        return ret

    def _get_temp_file(self, realfile):

        (_dirname, _filename) = self.os.path.split(realfile)

        _PC_NAME_MAX = self.os.pathconf(_dirname, self.os.pathconf_names['PC_NAME_MAX'])
        if len(_filename) > _PC_NAME_MAX:
            # raise IOError: File name too long
            open(_filename)

        _oversize = len(_filename) + 9 - _PC_NAME_MAX

        if _oversize > 0:
            _filename = _filename[:-_oversize]

        (fd, _tempfile) = tempfile.mkstemp(prefix=_filename + '.', dir=_dirname)
        self.os.close(fd)

        return _tempfile

    def open_copy(self, realfile):

        tempfile = self._get_temp_file(realfile)
        stat = self._safe_stat(realfile)
        if stat:
            shutil.copy2(realfile, tempfile)
            self.os.chown(tempfile, stat.st_uid, stat.st_gid)

        return (tempfile, stat)

    def open_trunc(self, realfile):

        tempfile = self._get_temp_file(realfile)
        stat = self._safe_stat(realfile)
        if stat:
            self.os.chown(tempfile, stat.st_uid, stat.st_gid)
            self.os.chmod(tempfile, stat.st_mode)

        return (tempfile, stat)

    def _safe_stat(self, path):
        stat = None
        try:
            stat = self.os.stat(path)
        except OSError as e:
            if e.errno != self.errno.ENOENT:
                raise e
        return stat

    def _safe_unlink(self, path):

        try:
            self.os.unlink(path)
        except OSError as e:
            self.logging.debug(str(e))
            if e.errno != self.errno.ENOENT:
                raise e

    def __check_stat(self, realfile):

        old_stat = self.files[realfile]['stat']
        cur_stat = self._safe_stat(realfile)

        if bool(old_stat) != bool(cur_stat):
            msg = 'transaction aborted file %s stat changed %d (%d)' % (
                realfile, bool(cur_stat), bool(old_stat))
            raise FileTransactionException(msg)

        if cur_stat:

            if old_stat.st_ino != cur_stat.st_ino:
                msg = 'transaction aborted file %s inode changed %d (%d)' % (
                    realfile, cur_stat.st_ino, old_stat.st_ino)
                raise FileTransactionException(msg)

            if old_stat.st_mtime != cur_stat.st_mtime:
                msg = 'transaction aborted file %s mtime changed %d (%d)' % (
                    realfile, cur_stat.st_mtime, old_stat.st_mtime)
                raise FileTransactionException(msg)

            if old_stat.st_size != cur_stat.st_size:
                msg = 'transaction aborted file %s size changed %d (%d)' % (
                    realfile, cur_stat.st_size, old_stat.st_size)
                raise FileTransactionException(msg)

    def commit(self):

        for realfile in self.files:
            for fp in self.files[realfile]['fp']:
                fp.close()

            if 'stat' in self.files[realfile]:
                self.__check_stat(realfile)

        self.dirs = set()

        for realfile in self.files:
            if 'tempfile' in self.files[realfile]:
                self.os.rename(self.files[realfile]['tempfile'], realfile)

        self.files = {}

    def rollback(self):

        for realfile in self.files:
            for fp in self.files[realfile]['fp']:
                fp.close()
            if 'tempfile' in self.files[realfile]:
                self._safe_unlink(self.files[realfile]['tempfile'])

        for d in sorted(self.dirs, key=lambda x: len(x.split(self.os.path.sep)), reverse=True):
            try:
                self.os.rmdir(d)
            except OSError as e:
                self.logging.debug(str(e))

    def __del__(self):
        self.rollback()

if __name__ == '__main__':
    ftrans = FileTransaction()
    ftrans.mkdir('dir')
    fp = ftrans.open('dir/file', 'w')
    fp.write("data")
    ftrans.commit()
