#!/usr/bin/python

import os
import shutil
import tempfile
import unittest
import random
import string

from filetransaction import FileTransaction

class MainTest(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='filetransaction.')

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def create_file(self, filename, data):
        fp = open(filename, 'w')
        fp.write(data)
        fp.close()

    def test_commit(self):

        init_data = "init data,"
        data1 = "test data one"
        data2 = "test data two"
        file1 = os.path.join(self.tempdir, "file1.dat")
        file2 = os.path.join(self.tempdir, "file2.dat")

        self.create_file(file1, init_data)

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

        self.create_file(file1, init_data)

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

    def test_name_max(self):

        name_max = os.pathconf(self.tempdir, os.pathconf_names['PC_NAME_MAX'])

        file1 = os.path.join(
            self.tempdir,
            ''.join(random.choice(string.letters) for _ in xrange(name_max)))
        ftrans = FileTransaction()
        ftrans.open(file1, "w")
        ftrans.commit()
        self.assertTrue(os.path.exists(file1))

        file2 = file1 + random.choice(string.letters)
        ftrans = FileTransaction()
        self.assertRaises(IOError, ftrans.open, file2, "w")

    def test_one_file_commit(self):

        testfile = os.path.join(self.tempdir, 'testfile.dat')
        self.create_file(testfile, 'one')

        ftrans = FileTransaction()

        fp = ftrans.open(testfile, 'r')
        self.assertEqual(fp.read(), 'one')

        fp = ftrans.open(testfile, 'w')
        fp.write('one,two')
        fp.close()
        fp = ftrans.open(testfile, 'r')
        self.assertEqual(fp.read(), 'one,two')

        fp = ftrans.open(testfile, 'a')
        fp.write(',three')
        fp.close()
        fp = ftrans.open(testfile, 'r')
        self.assertEqual(fp.read(), 'one,two,three')

        ftrans.commit()

        self.assertEqual(open(testfile, 'r').read(), 'one,two,three')

    def test_one_file_rollback(self):

        testfile = os.path.join(self.tempdir, 'testfile.dat')
        self.create_file(testfile, 'one')

        ftrans = FileTransaction()

        fp = ftrans.open(testfile, 'r')
        self.assertEqual(fp.read(), 'one')

        fp = ftrans.open(testfile, 'w')
        fp.write('one,two')
        fp.close()
        fp = ftrans.open(testfile, 'r')
        self.assertEqual(fp.read(), 'one,two')

        fp = ftrans.open(testfile, 'a')
        fp.write(',three')
        fp.close()
        fp = ftrans.open(testfile, 'r')
        self.assertEqual(fp.read(), 'one,two,three')

        ftrans.rollback()

        self.assertEqual(open(testfile, 'r').read(), 'one')

if __name__ == '__main__':
    unittest.main()
