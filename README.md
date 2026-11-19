# Bark

<img src="https://wx3.sinaimg.cn/mw690/0060lm7Tly1g0nfnjjxbbj30sg0sg757.jpg" width=200px height=200px />

[Bark](https://github.com/Finb/Bark) is an iOS App which allows you to push customed notifications to your iPhone.

## Installation

### For Docker User

![Docker Automated build](https://img.shields.io/docker/automated/finab/bark-server.svg) ![Image Size](https://img.shields.io/docker/image-size/finab/bark-server?sort=date) ![License](https://img.shields.io/github/license/finb/bark-server)

The docker image is already available, you can use the following command to run the bark server:

``` sh
docker run -dt --name bark -p 8080:8080 -v `pwd`/bark-data:/data finab/bark-server
```

You can also use the GitHub Container Registry image:

``` sh
docker run -dt --name bark -p 8080:8080 -v `pwd`/bark-data:/data ghcr.io/finb/bark-server
```

If you use the docker-compose tool, you can copy docker-copose.yaml under this project to any directory and run it:

``` sh
mkdir bark-server && cd bark-server
curl -sL https://github.com/Finb/bark-server/raw/master/deploy/docker-compose.yaml > docker-compose.yaml
docker compose up -d
```

### For General User 

- 1、Download precompiled binaries from the [releases](https://github.com/Finb/bark-server/releases) page
- 2、Add executable permissions to the bark-server binary: `chmod +x bark-server`
- 3、Start bark-server: `./bark-server --addr 0.0.0.0:8080 --data ./bark-data`
- 4、Test the server: `curl localhost:8080/ping`

**Note: Bark-server uses the `/data` directory to store data by default. Make sure that bark-server has permission to write to the `/data` directory, otherwise use the `-d` option to change the directory.**

### For Developer

Developers can compile this project by themselves, and the dependencies required for compilation:

- Golang 1.18+
- Go Mod Enabled(env `GO111MODULE=on`)
- Go Mod Proxy Enabled(env `GOPROXY=https://goproxy.cn`)
- [go-task](https://taskfile.dev/installation/) Installed

Run the following command to compile this project:

```sh
# Cross compile all platforms
task

# Compile the specified platform (please refer to Taskfile.yaml)
task linux_amd64
task linux_amd64_v3
```

**Note: The linux amd64 v3 architecture was added in go 1.18, see [https://github.com/golang/go/wiki/MinimumRequirements#amd64](https://github.com/golang/go/wiki/MinimumRequirements#amd64)**

### Use MySQL instead of Bbolt

Just run the server with `-dsn=user:pass@tcp(mysql_host)/bark`, it will use MySQL instead of file database Bbolt

## Android devices (WebSocket + HTTP long-poll fallback)

iOS/watchOS devices receive pushes through APNs. Android devices cannot use
APNs, so this fork delivers their messages over a persistent WebSocket, with
an HTTP long-poll fallback for networks that block long-lived sockets. The
Bark HTTP API (`/:key/...`, `/push`) stays identical; the server picks the
channel from the registered `device_type`.

1. **Register** (the Android app generates a random `device_token`):

   ```sh
   curl -X POST http://host:8080/register \
        -H 'Content-Type: application/json' \
        -d '{"device_token":"<random-uuid>","device_type":"android"}'
   # -> { "data": { "key": "xxxxxxxx", "device_type": "android", ... } }
   ```

2. **Primary channel — WebSocket.** Keep this open and rely on the 30s ping
   frames; push envelopes arrive as:

   ```
   ws://host:8080/ws?device_key=<key>
   ```

   ```json
   {"type":"message","id":"<uuid>","timestamp":1700000000,
    "data":{"id":"...","device_key":"...","title":"...","body":"...",
            "sound":"1107.caf","ext_params":{"group":"...","url":"..."}}}
   ```

   The full Bark parameter set is forwarded verbatim in `ext_params`.

3. **Fallback channel — HTTP long polling.** When the WebSocket cannot be
   established, the client repeatedly calls `/fetch`. The request is held open
   up to 20s and returns one envelope as soon as one is available, or `204` on
   timeout. After displaying a message the client confirms it with `/ack`;
   unacked messages regain visibility after a 60s lease and are redelivered.

   ```sh
   # long poll (block until a message or 204)
   curl "http://host:8080/fetch?device_key=<key>"
   # acknowledge a delivered message id
   curl -X POST http://host:8080/ack \
        -H 'Content-Type: application/json' \
        -d '{"device_key":"<key>","id":"<message-id>"}'
   # inspect the buffered count
   curl "http://host:8080/queue?device_key=<key>"
   ```

4. **Offline durability.** When a push arrives and no WebSocket is connected,
   the message is persisted in a bbolt queue (`data/queue.db`) instead of being
   rejected. It survives container restarts and is delivered on the next
   `/fetch` (or reconnected WebSocket). Undelivered messages expire after the
   queue TTL (default 7 days, `--queue-ttl` / `BARK_SERVER_QUEUE_TTL`); a
   background janitor sweeps expired entries hourly.

The long-poll hold time (20s) requires the server write timeout to stay above
it; the default `--write-timeout` was raised to 35s accordingly. APNs delivery
for iOS is unchanged.

## Others

* [API_V2.md](docs/API_V2.md).
* [MCP.md](docs/MCP.md).

