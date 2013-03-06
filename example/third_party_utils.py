# -*- coding: utf-8 -*-
# From http://effbot.org/librarybook/os-path-walk-example-3.py
#
# Copyright © 1995-2010 by Fredrik Lundh
#
# By obtaining, using, and/or copying this software and/or its associated
# documentation, you agree that you have read, understood, and will
# comply with the following terms and conditions:
#
# Permission to use, copy, modify, and distribute this software and its
# associated documentation for any purpose and without fee is hereby
# granted, provided that the above copyright notice appears in all copies,
# and that both that copyright notice and this permission notice appear
# in supporting documentation, and that the name of Secret Labs AB or
# the author not be used in advertising or publicity pertaining to
# distribution of the software without specific, written prior permission.
#
# SECRET LABS AB AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE, INCLUDING ALL IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO
# EVENT SHALL SECRET LABS AB OR THE AUTHOR BE LIABLE
# FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES
# OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF
# USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
# NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR
# IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import os

class DirectoryWalker:
    # a forward iterator that traverses a directory tree

    def __init__(self, directory):
        self.stack = [directory]
        self.files = []
        self.index = 0

    def __getitem__(self, index):
        while 1:
            try:
                a_file = self.files[self.index]
                self.index += 1
            except IndexError:
                # pop next directory from stack
                self.directory = self.stack.pop()
                self.files = os.listdir(self.directory)
                self.index = 0
            else:
                # got a filename
                fullname = os.path.join(self.directory, a_file)
                if os.path.isdir(fullname) and not os.path.islink(fullname):
                    self.stack.append(fullname)
                return fullname
