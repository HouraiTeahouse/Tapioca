# Tapioca [![Travis CI](https://api.travis-ci.org/HouraiTeahouse/Tapioca.svg?branch=master)](https://travis-ci.org/HouraiTeahouse/Tapioca)

Tapioca is Hourai Teahouse's game deployment system.  Inspired by Valve's
SteamPipe upload system, Tapioca is built to to efficently and continuously
deploy large binary games to players' computers.

## Features

 * Continuous deployment of games builds to player.
 * Support for multiple deployment branches. Useful for seperating the mainline.
 * Maximized bandwidth efficiency - downloads only changed portions of games for
   each patch.
 * Minimizes storage costs by deduplicating redundant data across builds and
   branches.
 * Leverages HTTP caching - built to maximize usage of content delivery networks
   (CDNs).
 * Can operate with no user-facing server - minimizes costs on server usage.

## Architecture

The process of pushing new game builds involves the following processes:

 * Downloading build artifacts from a continuous integration enviroment.
 * Chunking up build artifacts into roughly equal sized blocks.
 * Hashing each block to generate a unique block identifier.
 * Creating a manifest to describe binary builds using these blocks.
 * Uploading compressed blocks and manifest to a static file server or object
   store.

Tapioca clients check the game manifests on startup and ensure that the local
copy is up to date with the latest build. If a new build is available, only the
changed blocks are downloaded and patched into the local copy.

Blocks are uniquely identified by their hash. Tapioca currently uses SHA-512 as
its hashing algorithm. This has some nice properties:

 * This identifier is immutable for a given block, making it reasonable to
   aggressively cache the blocks in CDNs and other intermediate HTTP caches.
 * Identical blocks always have the same hash. This allows storing only one copy
   of the block for multiple builds and branches across a project.
 * As any machine is capable of running the hash, clients can independently
   verify the integrity of their local game files without needing to make any
   network calls to a server.
 * For the purposes of deduplication, SHA-512 hashes are virtually guaranteed to
   be unique. As Tapioca stores and operates on 1MB blocks, one would likely
   need to store more data than the universe can hold before finding a effective
   block collision.
 * The SHA hash function family will soon see broad hardware acceleration
   support, making game verification on clients faster. SHA-512 also generally
   runs faster on 64-bit machines, compared to SHA-256.

Blocks are compressed on upload. By default Tapioca is set to `gzip -9` blocks
before upload to the object store.

To minimize bandwidth and storage costs, Hourai Teahouse's instance of Tapioca
utilizes Backblaze B2 as the backing block store, with Cloudflare as the public
facing CDN, but practically any AWS S3-style object store or static file server
works. B2 to CloudFlare bandwidth is free, so the only monetary costs in this
configuration only involve storage and requests.

Finally, as each client must make at least one HTTP request on every launch of a
game, to minimize the payload size of these requests, Tapioca uses compressed
Google ProtoBuffers as a binary message format to deliver game manifest updates.

## Development

```
TOOD(james7132): Document
```

## Potential Future Features

 * IPFS storage or P2P block downloads to decrease the number of requests made
   to the backing object store.
 * Support encrypting or signing blocks or entire builds.
