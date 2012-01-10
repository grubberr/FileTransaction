#!/usr/bin/python

import os
import errno
import shutil
import tempfile

OP_READ = 1
OP_COPY = 2
OP_TRUNC = 4

class FileTransaction:
    " FileTransaction "

    def __init__(self):
        self.files = {}

    def open(self, name, mode):
        " open file and add to transaction set "

        realfile = os.path.realpath(name)
        if realfile in self.files:
            raise Exception(name + ' is already in transaction set')

        _op_mode = self._get_op_mode(mode)

        if _op_mode == OP_READ:
            fp = open(realfile, mode)
            self.files[realfile] = { "mode": _op_mode, "fp": fp }
            return fp

        elif _op_mode == OP_COPY:
            ( _tempfile, _stat, fp ) = self.open_copy(realfile, mode)
            self.files[realfile] = { "mode": _op_mode, "fp": fp, "tempfile": _tempfile, "stat": _stat }
            return fp

        elif _op_mode == OP_TRUNC:
            ( _tempfile, _stat, fp ) = self.open_trunc(realfile, mode)
            self.files[realfile] = { "mode": _op_mode, "fp": fp, "tempfile": _tempfile, "stat": _stat }
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

        ( _dirname, _filename ) = os.path.split(realfile)

        _PC_NAME_MAX = os.pathconf(_dirname, os.pathconf_names['PC_NAME_MAX'])
        if len(_filename) > _PC_NAME_MAX:
            # raise IOError: File name too long
            open(_filename)

        _oversize = len(_filename) + 7 - _PC_NAME_MAX

        if _oversize > 0:
            _filename = _filename[:-_oversize]

        ( fd, _tempfile ) = tempfile.mkstemp(prefix=_filename + '.', dir=_dirname)
        os.close(fd)

        return _tempfile

    def open_copy(self, realfile, mode):

        _tempfile = self._get_temp_file(realfile)
        _stat = self._stat_file(realfile)
        if _stat:
            shutil.copy2(realfile, _tempfile)

        return ( _tempfile, _stat, open(_tempfile, mode) )

    def open_trunc(self, realfile, mode):

        _tempfile = self._get_temp_file(realfile)
        _stat = self._stat_file(realfile)
        if _stat:
            os.chown(_tempfile, _stat.st_uid, _stat.st_gid)
            os.chmod(_tempfile, _stat.st_mode)

        return ( _tempfile, _stat, open(_tempfile, mode) )

    def _stat_file(self, filename):
        stat = None
        try:
            stat = os.stat(filename)
        except OSError, e:
            if e.errno != errno.ENOENT:
                raise e
        return stat

    def commit(self):

        for realfile in self.files:
            self.files[realfile]['fp'].close()
            if self.files[realfile]['mode'] in (OP_COPY, OP_TRUNC):
                old_stat = self.files[realfile]['stat']
                _stat = self._stat_file(realfile)
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
            if self.files[realfile]['mode'] in (OP_COPY, OP_TRUNC):
                _tempfile = self.files[realfile]['tempfile']
		os.rename(_tempfile, realfile)

    def rollback(self):

        for realfile in self.files:
            self.files[realfile]['fp'].close()
            if self.files[realfile]['mode'] in (OP_COPY, OP_TRUNC):
                os.unlink(self.files[realfile]['tempfile'])

if __name__ == '__main__':
    ftrans = FileTransaction()
    fp = ftrans.open('file', 'w')
    fp.write("data")
    ftrans.commit()
