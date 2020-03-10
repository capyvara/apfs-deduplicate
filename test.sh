#!/bin/bash
function finish {
    rm *.bin
}

trap finish EXIT

mkfile -nv 1g file0.bin
cp -v file0.bin file1.bin
cp -v file0.bin file2.bin
cp -v file0.bin file3.bin
cp -v file0.bin file4.bin
cp -v file0.bin file5.bin
rm -f file0.bin

du -shc *.bin

diskutil info /dev/disk1s1 | grep Space

./deduplicate.py "*.bin"

diskutil info /dev/disk1s1 | grep Space
