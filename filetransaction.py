#!/usr/bin/python

import shutil
import tempfile

OP_READ = 1
OP_COPY = 2
OP_TRUNC = 4

class FileTransaction:
    " FileTransaction "

    import os
    import errno
    import logging

    def __init__(self):
        self.files = {}

    def open(self, name, mode):
        " open file and add to transaction set "

        realfile = self.os.path.realpath(name)

        if realfile not in self.files:
            self.files[realfile] = {'fp': []}

        if 'tempfile' in self.files[realfile]:
            fp = open(self.files[realfile]['tempfile'], mode)
            self.files[realfile]['fp'].append(fp)
            return fp

        op_mode = self._get_op_mode(mode)

        if op_mode == OP_READ:
            fp = open(realfile, mode)
            self.files[realfile]['fp'].append(fp)
            return fp

        elif op_mode == OP_COPY:
            (tempfile, stat, fp) = self.open_copy(realfile, mode)

        elif op_mode == OP_TRUNC:
            (tempfile, stat, fp) = self.open_trunc(realfile, mode)

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

        ( _dirname, _filename ) = self.os.path.split(realfile)

        _PC_NAME_MAX = self.os.pathconf(_dirname, self.os.pathconf_names['PC_NAME_MAX'])
        if len(_filename) > _PC_NAME_MAX:
            # raise IOError: File name too long
            open(_filename)

        _oversize = len(_filename) + 7 - _PC_NAME_MAX

        if _oversize > 0:
            _filename = _filename[:-_oversize]

        ( fd, _tempfile ) = tempfile.mkstemp(prefix=_filename + '.', dir=_dirname)
        self.os.close(fd)

        return _tempfile

    def open_copy(self, realfile, mode):

        _tempfile = self._get_temp_file(realfile)
        _stat = self._safe_stat(realfile)
        if _stat:
            shutil.copy2(realfile, _tempfile)
            self.os.chown(_tempfile, _stat.st_uid, _stat.st_gid)

        return ( _tempfile, _stat, open(_tempfile, mode) )

    def open_trunc(self, realfile, mode):

        _tempfile = self._get_temp_file(realfile)
        _stat = self._safe_stat(realfile)
        if _stat:
            self.os.chown(_tempfile, _stat.st_uid, _stat.st_gid)
            self.os.chmod(_tempfile, _stat.st_mode)

        return ( _tempfile, _stat, open(_tempfile, mode) )

    def _safe_stat(self, path):
        stat = None
        try:
            stat = self.os.stat(path)
        except OSError, e:
            if e.errno != self.errno.ENOENT:
                raise e
        return stat

    def _safe_unlink(self, path):

        try:
            self.os.unlink(path)
        except OSError, e:
            self.logging.debug(str(e))
            if e.errno != self.errno.ENOENT:
                raise e

    def commit(self):

        for realfile in self.files:
            for fp in self.files[realfile]['fp']:
                fp.close()
            if 'tempfile' in self.files[realfile]:
                old_stat = self.files[realfile]['stat']
                _stat = self._safe_stat(realfile)
                if bool(old_stat) != bool(_stat):
                    msg = 'transaction aborted file %s stat changed %d (%d)' % (
                        realfile, bool(_stat), bool(old_stat))
                    raise Exception(msg)
                if _stat:
                    if old_stat.st_mtime != _stat.st_mtime:
                        msg = 'transaction aborted file %s mtime changed %d (%d)' % (
                            realfile, _stat.st_mtime, old_stat.st_mtime )
                        raise Exception(msg)
                    if old_stat.st_size != _stat.st_size:
                        msg = 'transaction aborted file %s size changed %d (%d)' % (
                            realfile, _stat.st_size, old_stat.st_size )
                        raise Exception(msg)

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

    def __del__(self):
        self.rollback()

if __name__ == '__main__':
    ftrans = FileTransaction()
    fp = ftrans.open('file', 'w')
    fp.write("data")
    ftrans.commit()
