FROM golang:1-alpine AS builder

# Module proxy; override with --build-arg GOPROXY=direct if needed.
ARG GOPROXY=https://goproxy.cn,direct
# All dependencies (bbolt, fiber, apns2, mysql driver) are pure Go:
# static build, no gcc/libc-dev/git needed in the builder image.
# -mod=mod lets go install fill missing go.sum entries from the local module
# cache (the COPY below overwrites the go.sum produced by the earlier download).
ENV GOPROXY=${GOPROXY} CGO_ENABLED=0 GOOS=linux GOFLAGS=-mod=mod

WORKDIR /go/src/github.com/finb/bark-server

# Dependencies first: this layer is cached unless go.mod/go.sum changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN set -ex \
    && BUILD_VERSION=$(git describe --tags 2>/dev/null || echo "dev") \
    && BUILD_DATE=$(date "+%F %T") \
    && COMMIT_SHA1=$(git rev-parse HEAD 2>/dev/null || echo "unknown") \
    && go install -trimpath -ldflags \
            "-X 'main.version=${BUILD_VERSION}' \
             -X 'main.buildDate=${BUILD_DATE}' \
             -X 'main.commitID=${COMMIT_SHA1}' \
             -w -s"

FROM alpine

LABEL maintainer="mritd <mritd@linux.com>"

# set up nsswitch.conf for Go's "netgo" implementation
# - https://github.com/golang/go/blob/go1.9.1/src/net/conf.go#L194-L275
# - docker run --rm debian:stretch grep '^hosts:' /etc/nsswitch.conf
RUN echo 'hosts: files dns' > /etc/nsswitch.conf

# default timezon
# override it with `--build-arg TIMEZONE=xxxx`
ARG TIMEZONE=Asia/Shanghai

ENV TZ=${TIMEZONE}

RUN set -ex \
    && sed -i 's|https://dl-cdn.alpinelinux.org|https://mirrors.aliyun.com|g' /etc/apk/repositories \
    && apk upgrade \
    && apk add --no-cache bash ca-certificates tzdata \
    && ln -sf /usr/share/zoneinfo/${TZ} /etc/localtime \
    && echo ${TZ} > /etc/timezone

COPY --from=builder /go/bin/bark-server /usr/local/bin/bark-server
COPY --from=builder /go/src/github.com/finb/bark-server/deploy/entrypoint.sh /entrypoint.sh

VOLUME /data

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]

CMD ["bark-server"]
