FROM rust:alpine3.22

RUN mkdir /home/hawkeye
WORKDIR /home/hawkeye
COPY Cargo.toml .
COPY src/ src/
RUN mkdir media

RUN apk add pkgconfig
RUN apk add openssl-dev
RUN apk add openssl-libs-static
RUN apk add libgcrypt

ENV TZ=Europe/Athens
RUN apk add --no-cache tzdata && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone

RUN cargo install --path .

CMD ["hawkeye"]
