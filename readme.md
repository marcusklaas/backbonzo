backbonzo
=========

[![Build Status](https://travis-ci.org/marcusklaas/backbonzo.svg?branch=master)](https://travis-ci.org/marcusklaas/backbonzo)

backbonzo is be a backup management tool written in Rust. It aims to

* be (reasonably) secure. To this end, it uses the AES256 cypher to encrypt the data and index before copying them to their destination.
* be fault tolerant. System crashes or network hangups should never corrupt your data. backbonzo only updates its index when a file has been backed up successfully. It also splits files into small blocks so that it won't lose much progress if it is interrupted.
* be efficient. Files are compressed before they are encrypted. It also provides a somewhat rudimentary form of deduplication by only storing unique blocks. This means that files with identical contents will only be stored once.
* keep track of changes. It only synchronizes new and changed files. Previous versions and deleted files are kept for a certain duration. It is possible to revert to any state in this period.
* expose its index. Command line programs do not provide the most user friendly interface. By storing the metadata in a straightforward sqlite database, other programs can easily provide graphical user interfaces (with the understanding that they possess the key).
* keep it simple. backbonzo fully relies on the abstractions the filesystem provides. It doesn't come with a dozen adapters for FTP, Dropbox, Amazon S3 or Google Drive. Nor does it provide clustering; backbonzo provides a single source, single destination backup procedure.
* do things concurrently. Creating encrypted backups involves computation and transportation. These can readily be done in parallel, so that one will never have to wait for the other.
* never crash or leak memory. All errors are properly handled where possible. Memory safety is enforced by the Rust compiler.

installation
------------

* install [rustc, cargo](http://www.rust-lang.org/install.html) *from the nightly channel* and the sqlite development headers
* `$ git clone https://github.com/marcusklaas/backbonzo.git`
* `$ cd backbonzo`
* `$ cargo build --release`
* `$ sudo cp target/release/backbonzo /usr/local/bin/backbonzo`

Note: backbonzo relies on [the comm crate](http://www.github.com/mahkoh/comm). Since this crate relies on unstable language features, we require the rust compiler from the nightly channel. 

usage
-----
There are three modes of operation: init, backup and restore. The init operation creates a hidden file in the source directory containing meta data. This file keeps track of the state of your backup. To set up a backup from `/home/user/important/` to `/var/backup/important`, run the following command:
```bash
$ backbonzo -s /home/user/important/ -d /var/backup/important
```
It will ask you for a passphrase which backbonzo will use to check that every backup is made with the same key. This passphrase is stored in a hashed form in the index file. Note that when the `-s` option is ommited, backbonzo will assume that the current working directory is meant.

After we've initialized our source directory, we can start backing up by executing
```bash
$ backbonzo -s /home/user/important/
```
It will remember the destination we gave it earlier. This command will also remove backups of old versions files that are no longer used. There are a few relevant options for the backup command. The most important ones are `--timeout` (or `-T` for short) and `--age` (`-a`
for short). The former makes backbonzo exit shortly after a specified number of seconds. After the timeout, backbonzo will only finish its current transfer and update the index file. The `--age` option specifies how long an old version of a file must have been overwritten before its backup is removed. The default value is 183, or half a year. This means that you can always revert your backed up directory to any previous state up to half a year ago.

To restore a backup, there's the `restore` subcommand. Its only required option is the location of the backup. Other relevant options are `--timestamp`/`-t` and `--filter`/ `-f`. The timestamp option specifies the point in time to in *milliseconds after the [UNIX epoch](https://en.wikipedia.org/wiki/Unix_time)*. The filter option is a [glob filter](https://en.wikipedia.org/wiki/Glob_%28programming%29) on the filenames to restore. For example, to restore the backup of the images subdirectory as of its state on June 29th, 2015 into the current directory, the following command is appropriate:
```bash
$ backbonzo -d /var/backup/important --timestamp=1435608987000 --filter=images/**
```

For a list of subcommands and options, run
```bash
$ backbonzo --help
```

security concerns
-----------------
backbonzo relies on the very awesome [rust-crypto](https://github.com/dagenix/rust-crypto/) crate for its cryptography primitives. It provides no guarantees for correctness or absence of vulnerabilities. But that is the least of our concerns right now. The project is in great shape, with high quality code base and a decent test suite.

The backbonzo currently leaks information on your data. Because backbonzo splits every file into blocks of fixed size without padding, any one with access to your encrypted data can fairly easily get a good idea of the number of distinct files in your data. Since it is unlikely that the number of bytes in a file is an multiple of the block size, the number of encrypted blocks which are smaller than the largest encrypted block is a fair estimator for the number of files. This is a very serious issue. The average file size of your backup reveals a lot about your data. It could tell you are storing mostly videos, images or small log files.

backbonzo implements compression on a per-block basis. This means that before the data in a given block is encrypted, it is run through a compression algorithm such as zlib or lzma. As a result, it may be very slightly more difficult to determine the number of files. On the other hand, this causes backbonzo to leak information on the compressibility of your data, which is even more telling of the data's nature.

What's worse is that the encrypted index file (metadata) is copied along with the actual data. An attacker could fairly easily use the size of this file to gain extra information. The way the index file is populated is very structured and predictable. The size of the index file could be combined with knowledge of the number of files to construct estimators for the number of directories, for example.

license
-------

MIT.
