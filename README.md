Twitter/GNIP Stream API Connector
=================================

Version 1.2 (initial public release)

This API connector was created to work with the Twitter stream API and store
the data locally for later analysis. It has been adapted to work with GNIP as
well. You will need user names and passwords or tokens and secrets from the
corresponding services to make this tool work.

The maven build process (````maven package````) creates a single executable
jar file that contains all dependencies. Running it is as simple as:

````
java -jar stream-crawler-1.2.jar crawler-twitter.xml
````

If you want to acquire access tokens from Twitter easily, just run the
authorize-user.scala script from the scripts directory after you have
requested a consumer key and token (needs to be set in the script).

The crawler has a management interface that allows you to reconfigure it by
sending a modified configuration xml.

It has been in use at [streamdrill](https://streamdrill.com) for quite a while
and worked continuously for a very long time. It is very robust and will
automatically reconnect to the stream if it is disconnected (with backoff).

It is most useful in conjunction with the [stream-proxy](https://github.com/streamdrill/stream-proxy),
which provides a simple way to retrieve the acquired data without having to
connect to the original source multiple times.

License
=======

Copyright (c) 2015, streamdrill UG (haftungsbeschränkt)
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

