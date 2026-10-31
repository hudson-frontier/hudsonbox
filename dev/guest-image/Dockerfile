# Native builds only. Pins and architecture are supplied by build_guest_image.py.
ARG RUST_IMAGE=rust:1.92.0-bookworm@sha256:e90e846de4124376164ddfbaab4b0774c7bdeef5e738866295e5a90a34a307a2
ARG USERLAND_IMAGE=debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
FROM ${RUST_IMAGE} AS build
ARG HUDSON_ARCH
ARG HUDSON_SOURCE_REVISION
ENV CARGO_HOME=/usr/local/cargo RUSTUP_HOME=/usr/local/rustup RUSTUP_TOOLCHAIN=1.92.0 CARGO_BUILD_JOBS=4
WORKDIR /src
COPY dev/guest-image/pins.json dev/guest-image/pins.json
RUN snapshot=$(sed -n 's/.*"debian_snapshot": "\([^"]*\)".*/\1/p' dev/guest-image/pins.json) \
    && test -n "$snapshot" \
    && rm -f /etc/apt/sources.list.d/debian.sources \
    && printf 'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/%s/ bookworm main\n' "$snapshot" > /etc/apt/sources.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends python3 protobuf-compiler e2fsprogs bc flex bison libelf-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/ crates/
COPY proto/ proto/
RUN test "$(uname -m)" = "$HUDSON_ARCH" \
    && RUSTFLAGS='--remap-path-prefix=/src=/hudson-source -C debuginfo=0' \
       cargo build --locked --release -p sandbox-guest \
    && mkdir /out \
    && cp target/release/sandbox-guest /out/sandbox-guest \
    && cp Cargo.lock /out/Cargo.lock
COPY VERSION VERSION
COPY scripts/guest_image.py scripts/guest_image.py
COPY dev/guest-image/ dev/guest-image/
RUN python3 scripts/guest_image.py fetch-kernel --architecture "$HUDSON_ARCH" --directory /src/linux
RUN epoch=$(python3 -c 'import json;print(json.load(open("dev/guest-image/pins.json"))["source_date_epoch"])') \
    && cd /src/linux \
    && if [ "$HUDSON_ARCH" = aarch64 ]; then kernel_arch=arm64; else kernel_arch=x86_64; fi \
    && ARCH="$kernel_arch" scripts/kconfig/merge_config.sh -m .config /src/dev/guest-image/hardening.config \
    && make ARCH="$kernel_arch" olddefconfig \
    && python3 /src/scripts/guest_image.py check-kernel-config --config .config \
    && KBUILD_BUILD_TIMESTAMP="$(date -u -d "@$epoch")" KBUILD_BUILD_USER=hudson KBUILD_BUILD_HOST=builder KBUILD_BUILD_VERSION=1 \
       make -j4 ARCH="$kernel_arch" \
    && if [ "$HUDSON_ARCH" = aarch64 ]; then cp arch/arm64/boot/Image /out/kernel.bin; else cp vmlinux /out/kernel.bin; fi \
    && cp .config /out/kernel.config \
    && cp /src/linux.tar.xz /out/linux-source.tar.xz

FROM ${USERLAND_IMAGE} AS userland
ARG HUDSON_ARCH
COPY --from=build /out/sandbox-guest /init
COPY --from=build /out/sandbox-guest /usr/local/libexec/hudson/sandbox-guest
# Only the reviewed source-built binary reaches the dynamic linker.
RUN if [ "$HUDSON_ARCH" = aarch64 ]; then /lib/ld-linux-aarch64.so.1 --list /init; else /lib64/ld-linux-x86-64.so.2 --list /init; fi

FROM build AS package
ARG HUDSON_ARCH
ARG HUDSON_SOURCE_REVISION
COPY --from=userland / /rootfs
RUN python3 scripts/guest_image.py assemble --architecture "$HUDSON_ARCH" \
    --source-revision "$HUDSON_SOURCE_REVISION" --rootfs /rootfs --output /out

# Export artifacts through an owned, never-started container; no runtime credentials.
FROM scratch AS artifacts
COPY --from=package /out/ /
