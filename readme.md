backbonzo
=========

[![Build Status](https://travis-ci.org/marcusklaas/backbonzo.svg?branch=master)](https://travis-ci.org/marcusklaas/backbonzo)

backbonzo is be a backup management tool written in Rust. It aims to

* be secure. To this end, it uses the AES256 cypher to encrypt the data and index before copying them to their destination.
* be fault tolerant. System crashes or network hangups should never corrupt your data. backbonzo only updates its index when a file has been backed up successfully. It also splits files into small blocks so that it won't lose much progress if it is interrupted.
* be efficient. Files are compressed before they are encrypted. It also provides a somewhat rudimentary form of deduplication by only storing unique blocks. This means that files with identical contents will only be stored once.
* keep track of changes. It only synchronizes new and changed files. Previous versions and deleted files are kept for a certain duration. It is possible to revert to any state in this period.
* run with zero configuration. Backing up your data should be as simple as running `backbonzo /home/user /media/vps/backup --key=ENCRYPTION_KEY`. It is however possible to provide a simple configuration in a TOML file.
* expose its index. Command line programs do not provide the most user friendly interface. By storing the metadata in a straightforward sqlite database, other programs can easily provide graphical user interfaces (with the understanding that they possess the key).
* keep it simple. backbonzo fully relies on the abstractions the filesystem provides. It doesn't come with a dozen adapters for FTP, Dropbox, Amazon S3 or Google Drive. Nor does it provide clustering; backbonzo provides a single source, single destination backup procedure.
* do things concurrently. Creating encrypted backups involves computation and transportation. These can readily be done in parallel, so that one will never have to wait for the other.

current state
-------------

Development on backbonzo has only just begun. At this time it is possible to create and restore encrypted backups. Functionality is extremely limited.

priority todo list
------------------


- [ ] add security concerns to readme
- [ ] add null alias for removed files
- [ ] ignore files with alias more recent than last modification
- [ ] deflate blocks before encryption
- [ ] implement partial decryption command
- [ ] take parameters/ read from configuration file
- [ ] add tests
- [ ] implement simple logging
- [ ] add clean up command
- [x] fix bug where helper thread sends on closed channel
- [x] when traversing filesystem, order by modification date
- [x] add timeout parameter
- [x] use a random initialization vector for each block
- [x] sort the error handling mess
- [x] seperate commands for initialization and ordinary backup
- [x] test correctness and reversibility of encryption
- [x] export index and config
- [x] implement total decryption command with timestamp
- [x] split file writing and encryption to different threads
- [x] check that encryption passwords are consistent
- [x] handle error juggling by implementing fromError trait for BonzoError

build
-----

* Install rustc, cargo and the sqlite development headers
* `$ git clone https://github.com/marcusklaas/backbonzo.git`
* `$ cd backbonzo`
* `$ cargo build`

license
-------

MIT or Apache2 probably. Maybe unlicense. Project name courtesy of [foswig.js](http://mrsharpoblunto.github.io/foswig.js/).
