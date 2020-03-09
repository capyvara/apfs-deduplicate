# README #

Deduplicate files on your APFS file system (or any file system that supports cloning files via `cp -c`

### What is this repository for? ###

* APFS allows cloning files instead of copying them
* This script detects duplicates and replaces them with clones

### How do I use it? ###

* Requires python3
* Run `./deduplicate.py` for help
* To save time, this script will first compile a list of probable duplicates by computing a hash of the first 1024 bytes of data. Of the probable matches, a hash of the full file contents are then computed, and duplicates are replaced with clones via calling `cp -c`
* Note: This script is considered experimental. Although it has been tested on a number of different data sets (git repositories, RDBMS storage, etc.) it should not be run on sensitive data

#### Credits
https://bitbucket.org/dchevell/apfs-deduplicate/