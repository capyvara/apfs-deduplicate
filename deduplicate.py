#!/usr/bin/env python3
import hashlib
import os
import subprocess
import glob
from subprocess import CalledProcessError
import sys
from os.path import isfile, islink, join


def chunk_reader(fobj, chunk_size=65536):
    """Generator that reads a file in chunks of bytes"""
    while True:
        chunk = fobj.read(chunk_size)
        if not chunk:
            return
        yield chunk


def get_hash(filename, first_chunk_only=False, hash=hashlib.sha1):
    hashobj = hash()
    try:
        file_object = open(filename, 'rb')
    except PermissionError:
        return

    if first_chunk_only:
        hashobj.update(file_object.read(1024))
    else:
        for chunk in chunk_reader(file_object):
            hashobj.update(chunk)
    hashed = hashobj.hexdigest()

    file_object.close()
    return hashed


def check_for_duplicates(paths, hash=hashlib.sha1):
    hashes_by_size = {}
    hashes_on_1k = {}
    hashes_full = {}

    for path in paths:
        print("Scanning %s ..." % (path)) 
        for path in glob.iglob(path, recursive=True):
            full_path = os.path.abspath(path)
            if not isfile(full_path) or islink(full_path):
                continue
            
            try:
                file_size = os.path.getsize(full_path)
            except (OSError,):
                # not accessible (permissions, etc)
                continue

            if file_size < 1024:
                continue
            
            duplicate = hashes_by_size.get(file_size)

            if duplicate:
                hashes_by_size[file_size].append(full_path)
            else:
                hashes_by_size[file_size] = []  # create the list for this file size
                hashes_by_size[file_size].append(full_path)

    # For all files with the same file size, get their hash on the 1st 1024 bytes
    print("Hashing headers...")
    visited_dirs = set()
    for __, files in hashes_by_size.items():
        if len(files) < 2:
            continue    # this file size is unique, no need to spend cpu cycles on it

        for filename in files:
            dirname = os.path.dirname(filename)
            if dirname not in visited_dirs:
                # print("Header hashing %s/ ..." % (dirname)) 
                visited_dirs.add(dirname)

            small_hash = get_hash(filename, first_chunk_only=True)

            duplicate = hashes_on_1k.get(small_hash)
            if duplicate:
                hashes_on_1k[small_hash].append(filename)
            else:
                hashes_on_1k[small_hash] = []          # create the list for this 1k hash
                hashes_on_1k[small_hash].append(filename)

    # For all files with the hash on the 1st 1024 bytes, get their hash on the full file - collisions will be duplicates
    print("Hashing...")
    visited_dirs = set()
    for __, files in hashes_on_1k.items():
        if len(files) < 2:
            continue    # this hash of fist 1k file bytes is unique, no need to spend cpu cycles on it

        for filename in files:
            dirname = os.path.dirname(filename)
            if dirname not in visited_dirs:
                # print("Hashing %s/ ..." % (dirname)) 
                visited_dirs.add(dirname)

            full_hash = get_hash(filename, first_chunk_only=False)

            duplicate = hashes_full.get(full_hash)
            if duplicate:
                duplicate = hashes_full[full_hash].append(filename)
            else:
                hashes_full[full_hash] = []          # create the list for this 1k hash
                hashes_full[full_hash].append(filename)

    print("Found %d  clusters." % (len(hashes_full)))

    total_bytes = 0
    unique_bytes = 0

    # Issue dedup cp's
    print("Deduping...")
    for full_hash, files in hashes_full.items():
        if len(files) < 2:
            continue    # this hash of fist 1k file bytes is unique, no need to spend cpu cycles on it
        
        duplicate = files[0]
        file_size = os.path.getsize(duplicate)
        total_bytes += file_size * len(files)
        unique_bytes += file_size
        print("Hash:%s Size:%d" % (full_hash, file_size))
        for filename in files:
            if filename == duplicate:
                print("\t> %s" % (filename))
                continue

            print("\t%s" % (filename))

            try:
                copyCommand = subprocess.run(["cp", "-cv", duplicate, filename], stdout=subprocess.PIPE, check=True)
                #print(copyCommand)
            except CalledProcessError:
                print('Could not dedupe file: %s. Skipping ...' % filename)

    print("Total deduped: %d bytes" % (total_bytes - unique_bytes))

if sys.argv[1:]:
    check_for_duplicates(sys.argv[1:])
else:
    print("Please pass the paths to check as parameters to the script")