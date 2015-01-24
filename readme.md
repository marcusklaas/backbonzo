backbonzo
=========

[![Build Status](https://travis-ci.org/marcusklaas/backbonzo.svg?branch=master)](https://travis-ci.org/marcusklaas/backbonzo)

backbonzo is be a backup management tool written in Rust. It aims to

* be (reasonably) secure. To this end, it uses the AES256 cypher to encrypt the data and index before copying them to their destination.
* be fault tolerant. System crashes or network hangups should never corrupt your data. backbonzo only updates its index when a file has been backed up successfully. It also splits files into small blocks so that it won't lose much progress if it is interrupted.
* be efficient. Files are compressed before they are encrypted. It also provides a somewhat rudimentary form of deduplication by only storing unique blocks. This means that files with identical contents will only be stored once.
* keep track of changes. It only synchronizes new and changed files. Previous versions and deleted files are kept for a certain duration. It is possible to revert to any state in this period.
* run with zero configuration. Backing up your data should be as simple as running `backbonzo /home/user /media/vps/backup --key=ENCRYPTION_KEY`. It is however possible to provide a simple configuration in a TOML file.
* expose its index. Command line programs do not provide the most user friendly interface. By storing the metadata in a straightforward sqlite database, other programs can easily provide graphical user interfaces (with the understanding that they possess the key).
* keep it simple. backbonzo fully relies on the abstractions the filesystem provides. It doesn't come with a dozen adapters for FTP, Dropbox, Amazon S3 or Google Drive. Nor does it provide clustering; backbonzo provides a single source, single destination backup procedure.
* do things concurrently. Creating encrypted backups involves computation and transportation. These can readily be done in parallel, so that one will never have to wait for the other.
* never crash or leak memory. All errors are properly handled where possible. Memory safety is enforced by the Rust compiler. Backbonzo never uses more than 3 megabytes of memory (when the block size is 1M).

security concerns
-----------------
backbonzo relies on the very awesome [rust-crypto](https://github.com/dagenix/rust-crypto/) crate for its cryptography primitives. It provides no guarantees for correctness or absence of vulnerabilities. But that is the least of our concerns right now. The project is in great shape, with high quality code base and a decent test suite.

The backbonzo currently leaks massive amounts of information on your data. Because backbonzo splits every file into blocks of fixed size without padding, any one with access to your encrypted data can fairly easily get a good idea of the number of distinct files in your data. Since it is unlikely that the number of bytes in a file is an multiple of the block size, the number of encrypted blocks which are smaller than the largest encrypted block is a fair estimator for the number of files. This is a very serious issue. The average file size of your backup reveals a lot about your data. It could tell you are storing mostly videos, images or small log files.

Soon, backbonzo will implement compression on a per-block basis. This means that before the data in a given block is encrypted, it is run through a compression algorithm such as zlib or lzma. As a result, it may be very slightly more difficult to determine the number of files. On the other hand, this change will cause backbonzo to leak information on the compressibility of your data, which is even more telling of the data's nature.

What's worse is that the encrypted index file (metadata) is copied along with the actual data. An attacker could fairly easily use the size of this file to gain extra information. The way the index file is populated is very structured and predictable. The size of the index file could be combined with knowledge of the number of files to construct estimators for the number of directories, for example.

What can be done? The most apparent solution would be to pad every block so that it is of maximum size. We could do something similar for the index file, padding it up to the next multible of 50 megabytes for example. This would relieve most issues mentioned above. This has grave consequences for storage efficiency, on the other hand. We could be storing up to a full dummy block for every file in the data set. It also completely negates the effect of compressions. All the bytes that are saved by compression are padded back on later. This last issue could then be mitigated by compressing on a file-level, but this would destroy block-level deduplication. It may be worth the trade-off as the likelihood of finding two files where one is the at-least-block-sized prefix of the other, but not vice versa seems low. The inefficiency can clearly be mitigated by reducing the maximum block size. This has other disadvantages. It grows the index file inversely propertionally and index lookup times inversely quadratically.

An other solution to our security concerns would be to write all blocks to a single file. Without an index to tell you where a block starts and ends, it is impossible to discern any information from this file other than the total backup size -- barring vulnerabilities in AES, of course. It remains an open question how we would store our index file. This approach is not without difficulty either. Reading from such a file requires file pointer arithmetic, which is more involved than simple file based I/O. The greatest challenge would lie in the removal on unused blocks. This would require an entire rebase of both the file blob and index file.

Other people must have thought about this problem. I should read a book.

current state
-------------

At this time it is possible to create and restore encrypted backups. Functionality is limited. The code is untested and it is *not ready for actual use*. 

priority todo list
------------------

- [ ] write some documentation on how to use backbonzo
- [ ] add clean up command
- [x] add more functional tests
- [x] save the destination directory on init
- [x] use time crate instead of `get_filesystem_time()`
- [x] try implement a `reduce` method on `Iterator<type=Result<_,_>>`  
- [x] implement simple logging
- [x] take parameters
- [x] implement partial restoration
- [x] deflate blocks before encryption
- [x] find a way to deal with empty files
- [x] deal with renames of files
- [x] add null alias for removed files
- [x] add security concerns to readme
- [x] ignore files with alias more recent than last modification
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
* `$ cargo build --release`

license
-------

MIT or Apache2 probably. Maybe unlicense. Project name courtesy of [foswig.js](http://mrsharpoblunto.github.io/foswig.js/).
