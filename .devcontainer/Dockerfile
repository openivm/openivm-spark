# syntax=docker/dockerfile:1.7

ARG NODE_VERSION

FROM ubuntu:22.04@sha256:f64b2ffc2f10ab8f8505aa56b2664bd846cbb9df87c7e2532e30f7667021c0e6 AS openivm-builder

ARG DUCKDB_CLI_SHA256
ARG DUCKDB_CLI_VERSION
ARG DUCKDB_COMMIT
ARG DUCKDB_REF
ARG EXTENSION_CI_TOOLS_COMMIT
ARG LPTS_BRANCH
ARG LPTS_COMMIT
ARG LPTS_REPO
ARG NATIVE_BUILD_JOBS
ARG OPENIVM_BRANCH
ARG OPENIVM_COMMIT
ARG OPENIVM_REPO
ARG LPTS_DUCKLAKE_COMMIT
ARG UBUNTU_MIRROR
ARG VCPKG_COMMIT
ARG VCPKG_REPO

ENV DEBIAN_FRONTEND=noninteractive

RUN if [ "${UBUNTU_MIRROR}" != "http://archive.ubuntu.com/ubuntu" ]; then \
      sed -i \
        "s|http://archive.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g; s|http://security.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g" \
        /etc/apt/sources.list; \
    fi \
    && apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=60 \
    && apt-get install -y --no-install-recommends ca-certificates curl gnupg \
    && curl -fsSL 'https://keyserver.ubuntu.com/pks/lookup?op=get&search=0xC8EC952E2A0E1FBDC5090F6A2C277A0A352154E5' -o /tmp/toolchain-key \
    && test "$(gpg --show-keys --with-colons /tmp/toolchain-key | awk -F: '$1 == "fpr" { print $10; exit }')" = C8EC952E2A0E1FBDC5090F6A2C277A0A352154E5 \
    && gpg --dearmor -o /usr/share/keyrings/ubuntu-toolchain.gpg /tmp/toolchain-key \
    && rm /tmp/toolchain-key \
    && echo 'deb [signed-by=/usr/share/keyrings/ubuntu-toolchain.gpg] https://ppa.launchpadcontent.net/ubuntu-toolchain-r/test/ubuntu jammy main' > /etc/apt/sources.list.d/ubuntu-toolchain.list \
    && apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=60 \
    && apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      gcc-13 \
      g++-13 \
      cmake \
      curl \
      git \
      libssl-dev \
      ninja-build \
      pkg-config \
      python3 \
      python3-pip \
      unzip \
      zip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

RUN --mount=type=cache,target=/root/.gitcache \
    git clone --branch "${OPENIVM_BRANCH}" --depth 50 "${OPENIVM_REPO}" . \
    && git fetch --depth 1 origin "${OPENIVM_COMMIT}" \
    && git checkout --detach "${OPENIVM_COMMIT}" \
    && test "$(git rev-parse HEAD)" = "${OPENIVM_COMMIT}" \
    && git config --global url."https://github.com/".insteadOf "git@github.com:" \
    && git config -f .gitmodules submodule.third_party/lpts.url "${LPTS_REPO}" \
    && git submodule sync --recursive \
    && git submodule update --init --recursive --depth 1 duckdb extension-ci-tools \
    && git -C duckdb fetch --depth 1 origin tag "${DUCKDB_REF}" \
    && git -C duckdb checkout --detach "${DUCKDB_COMMIT}" \
    && test "$(git -C duckdb rev-parse HEAD)" = "${DUCKDB_COMMIT}" \
    && git -C duckdb submodule update --init --recursive --depth 1 \
    && test "$(git -C extension-ci-tools rev-parse HEAD)" = "${EXTENSION_CI_TOOLS_COMMIT}" \
    && cd third_party/lpts \
    && git init -q \
    && (git remote add origin "${LPTS_REPO}" 2>/dev/null || git remote set-url origin "${LPTS_REPO}") \
    && git fetch --depth 50 origin "${LPTS_BRANCH}" \
    && git checkout --detach "${LPTS_COMMIT}" \
    && test "$(git rev-parse HEAD)" = "${LPTS_COMMIT}" \
    && git submodule update --init --recursive --depth 1 \
    && test "$(git -C third_party/ducklake rev-parse HEAD)" = "${LPTS_DUCKLAKE_COMMIT}" \
    && git clone --filter=blob:none --no-checkout "${VCPKG_REPO}" /opt/vcpkg \
    && git -C /opt/vcpkg fetch --depth 1 origin "${VCPKG_COMMIT}" \
    && git -C /opt/vcpkg checkout --detach "${VCPKG_COMMIT}" \
    && test "$(git -C /opt/vcpkg rev-parse HEAD)" = "${VCPKG_COMMIT}" \
    && /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics

RUN test "$(gcc-13 -dumpversion | cut -d. -f1)" = 13 \
    && CC=gcc-13 CXX=g++-13 VCPKG_ROOT=/opt/vcpkg \
       VCPKG_TOOLCHAIN_PATH=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake \
       USE_MERGED_VCPKG_MANIFEST=1 CMAKE_BUILD_PARALLEL_LEVEL="${NATIVE_BUILD_JOBS}" \
       GEN=ninja make release

RUN mkdir -p /artifacts /opt/downloads \
    && curl -fsSL \
       "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_CLI_VERSION}/duckdb-cli-linux-amd64.tar.gz" \
       -o /opt/downloads/duckdb-cli-linux-amd64.tar.gz \
    && printf '%s  %s\n' "${DUCKDB_CLI_SHA256}" /opt/downloads/duckdb-cli-linux-amd64.tar.gz \
       | sha256sum -c - \
    && tar -xzf /opt/downloads/duckdb-cli-linux-amd64.tar.gz -C /opt/downloads \
    && install -m 0755 /opt/downloads/duckdb /artifacts/duckdb \
    && cp build/release/extension/openivm/openivm.duckdb_extension /artifacts/ \
    && cd /artifacts \
    && sha256sum openivm.duckdb_extension duckdb > SHA256SUMS \
    && rm -rf /opt/downloads

FROM ubuntu:22.04@sha256:f64b2ffc2f10ab8f8505aa56b2664bd846cbb9df87c7e2532e30f7667021c0e6 AS native-runtime-check
COPY --from=openivm-builder /artifacts/ /artifacts/
RUN glibc_version="$(getconf GNU_LIBC_VERSION | cut -d" " -f2)" \
    && test "${glibc_version}" = 2.35 \
    && grep -aq 'GLIBCXX_3.4.30' /usr/lib/x86_64-linux-gnu/libstdc++.so.6 \
    && /artifacts/duckdb --version | grep -E '^v2\.0\.0([[:space:]]|$)' \
    && mkdir -p /artifacts/runtime \
    && /artifacts/duckdb -unsigned -noheader -csv -c "LOAD '/artifacts/openivm.duckdb_extension'; \
       SET openivm_files_path='/artifacts/runtime'; CREATE TABLE openivm_runtime_source(id INTEGER); \
       CREATE OR REPLACE MATERIALIZED VIEW openivm_runtime_probe AS SELECT id FROM openivm_runtime_source; \
       SELECT DISTINCT refresh_type_name FROM openivm_compile_with_facts('openivm_runtime_probe', \
       '{\"target_dialect\":\"spark\",\"compile_only\":true}');" | grep -Fx SIMPLE_PROJECTION

FROM eclipse-temurin:17-jdk-noble@sha256:b73c483fd77e77ad71b25cc146de5172bf9cf34339ccd8aad4996e1878280810 AS jdk17

FROM node:${NODE_VERSION}-bookworm-slim@sha256:48abc13a19400ca3985071e287bd405a1d99306770eb81d61202fb6b65cf0b57 AS node-runtime

FROM docker:27.5.1-cli@sha256:851f91d241214e7c6db86513b270d58776379aacc5eb9c4a87e5b47115e3065c AS docker-cli

FROM eclipse-temurin:21-jdk-noble@sha256:1afee26c547e1aa89f47404854dd09203b8bd35a2be269e16bdfd69c4a2c2b50 AS devcontainer

ARG NODE_VERSION
ARG NPM_VERSION
ARG GH_CLI_SHA256
ARG GH_CLI_VERSION
ARG SBT_VERSION
ARG TARGETARCH
ARG UBUNTU_MIRROR

RUN if [ "${TARGETARCH}" != "amd64" ]; then \
      echo "openivm-spark-devcontainer supports only linux/amd64; TARGETARCH=${TARGETARCH:-unset}" >&2; \
      exit 1; \
    fi

ENV DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/opt/java/jdk-17 \
    NPM_CONFIG_AUDIT=false \
    NPM_CONFIG_FUND=false \
    NPM_CONFIG_UPDATE_NOTIFIER=false \
    OPENIVM_CLI_PATH=/opt/openivm/duckdb \
    OPENIVM_EXTENSION_PATH=/opt/openivm/openivm.duckdb_extension \
    OPENIVM_JAVA_HOME_SPARK_35=/opt/java/jdk-17 \
    OPENIVM_JAVA_HOME_SPARK_41=/opt/java/jdk-21 \
    OPENIVM_NATIVE_DIR=/opt/openivm \
    OPENIVM_SPARK_TARGET=spark-3.5 \
    PATH=/opt/java/jdk-17/bin:/opt/openivm-node-tooling/node_modules/.bin:${PATH}

LABEL org.opencontainers.image.description="OpenIVM Spark development and CI toolchain" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.source="https://github.com/openivm/openivm-spark" \
      org.opencontainers.image.title="openivm-spark-devcontainer"

RUN if [ "${UBUNTU_MIRROR}" != "http://archive.ubuntu.com/ubuntu" ]; then \
      sed -i \
        "s|http://archive.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g; s|http://security.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g" \
        /etc/apt/sources.list.d/ubuntu.sources; \
    fi \
    && apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=60 \
    && apt-get install -y --no-install-recommends \
      bash \
      build-essential \
      ca-certificates \
      curl \
      git \
      jq \
      less \
      lsof \
      make \
      openssh-client \
      procps \
      python3 \
      python3-pip \
      rsync \
      sudo \
      unzip \
      zip \
    && rm -rf /var/lib/apt/lists/*

RUN printf '%s\n' 'py4j==0.10.9.7 --hash=sha256:85defdfd2b2376eb3abf5ca6474b51ab7e0de341c75a02f46dc9b5976f5a5c1b' \
      | python3 -m pip install --no-cache-dir --no-deps --only-binary=:all: --require-hashes \
          --target /opt/openivm-py4j/spark-3.5 -r /dev/stdin \
    && printf '%s\n' 'py4j==0.10.9.9 --hash=sha256:c7c26e4158defb37b0bb124933163641a2ff6e3a3913f7811b0ddbe07ed61533' \
      | python3 -m pip install --no-cache-dir --no-deps --only-binary=:all: --require-hashes \
          --target /opt/openivm-py4j/spark-4.1 -r /dev/stdin

COPY --from=jdk17 /opt/java/openjdk /opt/java/jdk-17
COPY --from=node-runtime /usr/local/ /usr/local/
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker-cli /usr/local/libexec/docker/cli-plugins/ /usr/local/libexec/docker/cli-plugins/
COPY --from=native-runtime-check /artifacts/ /opt/openivm/

RUN ln -s /opt/java/openjdk /opt/java/jdk-21 \
    && ln -s /opt/java/jdk-17 /opt/java/openjdk-17 \
    && existing_group="$(getent group 1000 | cut -d: -f1)" \
    && if getent group vscode >/dev/null 2>&1; then \
      test "$(getent group vscode | cut -d: -f3)" = "1000" \
        || { echo "Existing vscode group does not use required GID 1000" >&2; exit 1; }; \
    elif [ -n "${existing_group}" ]; then \
      groupmod --new-name vscode "${existing_group}"; \
    else \
      groupadd --gid 1000 vscode; \
    fi \
    && existing_user="$(getent passwd 1000 | cut -d: -f1)" \
    && if id vscode >/dev/null 2>&1; then \
      test "$(id -u vscode)" = "1000" \
        || { echo "Existing vscode user does not use required UID 1000" >&2; exit 1; }; \
    elif [ -n "${existing_user}" ]; then \
      usermod --login vscode "${existing_user}"; \
    else \
      useradd --uid 1000 --gid vscode --create-home --shell /bin/bash vscode; \
    fi \
    && if [ "$(getent passwd vscode | cut -d: -f6)" != "/home/vscode" ]; then \
      usermod --home /home/vscode --move-home vscode; \
    fi \
    && usermod --gid vscode --shell /bin/bash vscode \
    && printf 'vscode ALL=(root) NOPASSWD:ALL\n' > /etc/sudoers.d/vscode \
    && chmod 0440 /etc/sudoers.d/vscode \
    && mkdir -p /opt/downloads /opt/openivm-node-tooling /opt/sbt \
    && chown -R vscode:vscode /opt/openivm-node-tooling \
    && curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
      -o /opt/downloads/sbt.tgz \
    && printf '%s  %s\n' \
      '23543bc4597b552e8e2c27d695fe672ec235ab8a64f766b37828fb68c6d9d910' \
      '/opt/downloads/sbt.tgz' \
      | sha256sum -c - \
    && tar -xzf /opt/downloads/sbt.tgz -C /opt \
    && GH_CLI_ARCHIVE="/opt/downloads/gh_${GH_CLI_VERSION}_linux_${TARGETARCH}.tar.gz" \
    && curl -fsSL \
      "https://github.com/cli/cli/releases/download/v${GH_CLI_VERSION}/gh_${GH_CLI_VERSION}_linux_${TARGETARCH}.tar.gz" \
      -o "${GH_CLI_ARCHIVE}" \
    && printf '%s  %s\n' "${GH_CLI_SHA256}" "${GH_CLI_ARCHIVE}" | sha256sum -c - \
    && tar -xzf "${GH_CLI_ARCHIVE}" -C /opt/downloads \
    && install -m 0755 \
      "/opt/downloads/gh_${GH_CLI_VERSION}_linux_${TARGETARCH}/bin/gh" \
      /usr/local/bin/gh \
    && rm -rf /opt/downloads \
    && ln -s /opt/sbt/bin/sbt /usr/local/bin/sbt \
    && chown -R vscode:vscode /opt/openivm /opt/sbt /home/vscode

WORKDIR /opt/openivm-node-tooling

COPY --chown=vscode:vscode package.json package-lock.json ./

USER vscode

RUN HOME=/home/vscode test "$(node --version)" = "v${NODE_VERSION}" \
    && test "$(npm --version)" = "${NPM_VERSION}" \
    && HOME=/home/vscode npm ci

USER root

RUN for binary in devcontainer nx prettier tsc tsx; do \
      ln -s "/opt/openivm-node-tooling/node_modules/.bin/${binary}" "/usr/local/bin/${binary}"; \
    done

WORKDIR /opt/openivm-sbt

COPY --chown=vscode:vscode spark-ext/.sbtopts ./
COPY --chown=vscode:vscode spark-ext/build.sbt ./
COPY --chown=vscode:vscode spark-ext/project/ ./project/

RUN mkdir -p ivm-common ivm-compiler ivm-executor ivm-extension ivm-it \
    && chown -R vscode:vscode /opt/openivm-sbt

USER vscode

RUN HOME=/home/vscode OPENIVM_SPARK_TARGET=spark-3.5 JAVA_HOME=/opt/java/jdk-17 sbt -batch update \
    && HOME=/home/vscode OPENIVM_SPARK_TARGET=spark-4.1 JAVA_HOME=/opt/java/jdk-21 sbt -batch update

USER root

COPY --chmod=755 spark-ext/dev/docker/entrypoint.sh /usr/local/bin/openivm-entrypoint
COPY --chmod=755 .devcontainer/scripts/post-create.sh /usr/local/share/openivm-spark/post-create.sh

RUN cd /opt/openivm \
    && sha256sum -c SHA256SUMS \
    && /opt/openivm/duckdb --version \
    && /opt/java/jdk-17/bin/java -version \
    && /opt/java/jdk-21/bin/java -version \
    && gh --version \
    && sbt --script-version

ENV HOME=/home/vscode

USER vscode
WORKDIR /workspaces/openivm-spark

ENTRYPOINT ["/usr/local/bin/openivm-entrypoint"]
CMD ["sleep", "infinity"]
