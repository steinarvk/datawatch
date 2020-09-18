# datawatch

This tool stores incremental changes to binary data efficiently
using delta encoding. This makes it possible to keep track of
how a file has changed over time without using excessive storage.

The storage format is JSON-based. The actual delta encoding is
based on BSDIFF4.

Includes a simple web crawler to demonstrate and try out the
concept in practice, watching pages change on the web.

This is a pre-release version of the code. The API should not be
relied upon to remain stable.
