backbonzo
=========

backbonzo will be a backup management tool written in Rust. It aims to

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

Development on backbonzo has only just begun. It is certainly not functional yet. At the moment it scans the working directory for new and changed files and outputs encrypted blocks to `/tmp`. There is no way to recover a backup at this time.

priority todo list
------------------

- [x] sort the error handling mess
- [ ] take parameters/ read from configuration file
- [ ] seperate commands for initialization and ordinary backup
- [ ] add timeout parameter
- [ ] test correctness and reversibility of encryption
- [ ] export index and config
- [ ] implement decryption command (total, file) with timestamp
- [ ] split file writing and encryption to different threads

build
-----

* Install rustc, cargo and the sqlite development headers
* `$ git clone https://github.com/marcusklaas/backbonzo.git`
* `$ cd backbonzo`
* `$ cargo run`

license
-------

MIT or Apache2 probably. Project name courtesy of [http://mrsharpoblunto.github.io/foswig.js/](foswig.js).
