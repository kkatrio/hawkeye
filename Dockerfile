FROM rust:alpine3.23

RUN mkdir /home/hawkeye
WORKDIR /home/hawkeye
COPY Cargo.toml .
COPY src/ src/
COPY clientsecret.json clientsecret.json
RUN mkdir media

RUN apk add pkgconfig
RUN apk add openssl-dev
RUN apk add openssl-libs-static
RUN apk add libgcrypt

RUN cargo install --path .

CMD ["hawkeye"]
