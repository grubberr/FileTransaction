#!/usr/bin/python

import os
import shutil
import tempfile
import unittest

from filetransaction import FileTransaction

class MainTest(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='filetransaction.')

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_commit(self):

        init_data = "init data,"
        data1 = "test data one"
        data2 = "test data two"
        file1 = os.path.join(self.tempdir, "file1.dat")
        file2 = os.path.join(self.tempdir, "file2.dat")

        fp = open(file1, "w")
        fp.write(init_data)
        fp.close()

        ftrans = FileTransaction()
        fp1 = ftrans.open(file1, "a")
        fp2 = ftrans.open(file2, "w")
        fp1.write(data1)
        fp2.write(data2)
        ftrans.commit()
        self.assertEqual(open(file1).read(), init_data + data1)
        self.assertEqual(open(file2).read(), data2)

    def test_rollback(self):

        init_data = "init data,"
        data1 = "test data one"
        data2 = "test data two"
        file1 = os.path.join(self.tempdir, "file1.dat")
        file2 = os.path.join(self.tempdir, "file2.dat")

        fp = open(file1, "w")
        fp.write(init_data)
        fp.close()

        ftrans = FileTransaction()
        fp1 = ftrans.open(file1, "a")
        fp2 = ftrans.open(file2, "w")
        fp1.write(data1)
        fp2.write(data2)
        fp1.close()
        fp2.close()
        ftrans.rollback()

        self.assertEqual(open(file1).read(), init_data)
        self.assertFalse(os.path.exists(file2))

if __name__ == '__main__':
    unittest.main()
