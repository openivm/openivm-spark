# syntax=docker/dockerfile:1.7

# =============================================================================
#  openivm-spark — multi-stage development image.
#
#  Stages:
#    1. openivm-builder      Builds OpenIVM with the explicitly pinned LPTS
#                            sources as a DuckDB extension. Produces
#                            /artifacts/openivm.duckdb_extension and the
#                            checksummed official DuckDB CLI for explicit dynamic loading.
#    2. openivm-tester       Inherits stage 1's full build tree so `make test`
#                            can run the upstream sqllogictest suite + our new
#                            compile_refresh tests. Used as the docker-compose
#                            `openivm-test` service target.
#    3. spark-ext            Runtime image. JDK 17 + sbt + scala. Bind-mounts
#                            /home/mdrrahman/openivm-spark/spark-ext at /work
#                            and the prebuilt extension at /opt/openivm.
#
#  Build args (defaults match spark-ext/dev/pins.env):
#    OPENIVM_REPO, OPENIVM_COMMIT
#    LPTS_REPO,    LPTS_COMMIT
#    JDK_VERSION,  SBT_VERSION, SCALA_VERSION
#    DUCKDB_REF, DUCKDB_COMMIT, DUCKDB_CLI_VERSION, DUCKDB_CLI_SHA256
#    EXTENSION_CI_TOOLS_COMMIT, LPTS_DUCKLAKE_COMMIT, VCPKG_REPO, VCPKG_COMMIT
#    NATIVE_BUILD_JOBS, UBUNTU_MIRROR (optional)
# =============================================================================

ARG JDK_VERSION=17

# -----------------------------------------------------------------------------
# Stage 1: openivm-builder
# -----------------------------------------------------------------------------
FROM ubuntu:22.04@sha256:f64b2ffc2f10ab8f8505aa56b2664bd846cbb9df87c7e2532e30f7667021c0e6 AS openivm-builder

ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu
ARG OPENIVM_REPO
ARG OPENIVM_BRANCH=main
ARG OPENIVM_COMMIT
ARG LPTS_REPO
ARG LPTS_BRANCH=main
ARG LPTS_COMMIT
ARG DUCKDB_CLI_SHA256
ARG DUCKDB_CLI_VERSION=v2.0.0
ARG DUCKDB_REF
ARG DUCKDB_COMMIT
ARG EXTENSION_CI_TOOLS_COMMIT
ARG LPTS_DUCKLAKE_COMMIT
ARG NATIVE_BUILD_JOBS=16
ARG VCPKG_COMMIT
ARG VCPKG_REPO

ENV DEBIAN_FRONTEND=noninteractive

RUN if [ "${UBUNTU_MIRROR}" != "http://archive.ubuntu.com/ubuntu" ]; then \
        sed -i "s|http://archive.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g; s|http://security.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g" \
            /etc/apt/sources.list; \
    fi && \
    apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=60 && \
    apt-get install -y --no-install-recommends ca-certificates curl gnupg && \
    curl -fsSL 'https://keyserver.ubuntu.com/pks/lookup?op=get&search=0xC8EC952E2A0E1FBDC5090F6A2C277A0A352154E5' -o /tmp/toolchain-key && \
    test "$(gpg --show-keys --with-colons /tmp/toolchain-key | awk -F: '$1 == "fpr" { print $10; exit }')" = C8EC952E2A0E1FBDC5090F6A2C277A0A352154E5 && \
    gpg --dearmor -o /usr/share/keyrings/ubuntu-toolchain.gpg /tmp/toolchain-key && \
    rm /tmp/toolchain-key && \
    echo 'deb [signed-by=/usr/share/keyrings/ubuntu-toolchain.gpg] https://ppa.launchpadcontent.net/ubuntu-toolchain-r/test/ubuntu jammy main' > /etc/apt/sources.list.d/ubuntu-toolchain.list && \
    apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=60 && \
    apt-get install -y --no-install-recommends \
        build-essential gcc-13 g++-13 cmake ninja-build git ca-certificates \
        python3 python3-pip libssl-dev pkg-config zip unzip curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Clone OpenIVM at the pinned commit, then apply the explicit LPTS
# repository and revision selected by pins.env.
RUN --mount=type=cache,target=/root/.gitcache \
    git clone --branch "${OPENIVM_BRANCH}" --depth 50 "${OPENIVM_REPO}" . && \
    git fetch --depth 1 origin "${OPENIVM_COMMIT}" && \
    git checkout --detach "${OPENIVM_COMMIT}" && \
    test "$(git rev-parse HEAD)" = "${OPENIVM_COMMIT}" && \
    git config --global url."https://github.com/".insteadOf "git@github.com:" && \
    git config -f .gitmodules submodule.third_party/lpts.url "${LPTS_REPO}" && \
    git submodule sync --recursive && \
    git submodule update --init --recursive --depth 1 duckdb extension-ci-tools && \
    git -C duckdb fetch --depth 1 origin tag "${DUCKDB_REF}" && \
    git -C duckdb checkout --detach "${DUCKDB_COMMIT}" && \
    test "$(git -C duckdb rev-parse HEAD)" = "${DUCKDB_COMMIT}" && \
    git -C duckdb submodule update --init --recursive --depth 1 && \
    test "$(git -C extension-ci-tools rev-parse HEAD)" = "${EXTENSION_CI_TOOLS_COMMIT}" && \
    (cd third_party/lpts && \
        git init -q && \
        (git remote add origin "${LPTS_REPO}" 2>/dev/null || git remote set-url origin "${LPTS_REPO}") && \
        git fetch --depth 50 origin "${LPTS_BRANCH}" && \
        git checkout --detach "${LPTS_COMMIT}" && \
        test "$(git rev-parse HEAD)" = "${LPTS_COMMIT}" && \
        git submodule update --init --recursive --depth 1 && \
        test "$(git -C third_party/ducklake rev-parse HEAD)" = "${LPTS_DUCKLAKE_COMMIT}") && \
    git clone --filter=blob:none --no-checkout "${VCPKG_REPO}" /opt/vcpkg && \
    git -C /opt/vcpkg fetch --depth 1 origin "${VCPKG_COMMIT}" && \
    git -C /opt/vcpkg checkout --detach "${VCPKG_COMMIT}" && \
    test "$(git -C /opt/vcpkg rev-parse HEAD)" = "${VCPKG_COMMIT}" && \
    /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics

RUN test "$(gcc-13 -dumpversion | cut -d. -f1)" = 13 && \
    CC=gcc-13 CXX=g++-13 VCPKG_ROOT=/opt/vcpkg \
    VCPKG_TOOLCHAIN_PATH=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake \
    USE_MERGED_VCPKG_MANIFEST=1 CMAKE_BUILD_PARALLEL_LEVEL="${NATIVE_BUILD_JOBS}" \
    GEN=ninja make release

RUN mkdir -p /artifacts /opt/downloads && \
    curl -fsSL \
        "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_CLI_VERSION}/duckdb-cli-linux-amd64.tar.gz" \
        -o /opt/downloads/duckdb-cli-linux-amd64.tar.gz && \
    printf '%s  %s\n' "${DUCKDB_CLI_SHA256}" /opt/downloads/duckdb-cli-linux-amd64.tar.gz | sha256sum -c - && \
    tar -xzf /opt/downloads/duckdb-cli-linux-amd64.tar.gz -C /opt/downloads && \
    install -m 0755 /opt/downloads/duckdb /artifacts/duckdb && \
    cp build/release/extension/openivm/openivm.duckdb_extension /artifacts/ && \
    cd /artifacts && \
    sha256sum openivm.duckdb_extension duckdb > SHA256SUMS && \
    rm -rf /opt/downloads

FROM ubuntu:22.04@sha256:f64b2ffc2f10ab8f8505aa56b2664bd846cbb9df87c7e2532e30f7667021c0e6 AS native-runtime-check
COPY --from=openivm-builder /artifacts/ /artifacts/
RUN glibc_version="$(getconf GNU_LIBC_VERSION | cut -d" " -f2)" && \
    test "${glibc_version}" = 2.35 && \
    grep -aq 'GLIBCXX_3.4.30' /usr/lib/x86_64-linux-gnu/libstdc++.so.6 && \
    /artifacts/duckdb --version | grep -E '^v2\.0\.0([[:space:]]|$)' && \
    mkdir -p /artifacts/runtime && \
    /artifacts/duckdb -unsigned -noheader -csv -c "LOAD '/artifacts/openivm.duckdb_extension'; \
        SET openivm_files_path='/artifacts/runtime'; CREATE TABLE openivm_runtime_source(id INTEGER); \
        CREATE OR REPLACE MATERIALIZED VIEW openivm_runtime_probe AS SELECT id FROM openivm_runtime_source; \
        SELECT DISTINCT refresh_type_name FROM openivm_compile_with_facts('openivm_runtime_probe', \
        '{\"target_dialect\":\"spark\",\"compile_only\":true}');" | grep -Fx SIMPLE_PROJECTION

# -----------------------------------------------------------------------------
# Stage 2: openivm-tester — `make test` over the OpenIVM source tree.
#
# This stage exists so `docker compose run --rm openivm-test` exercises the
# upstream sqllogictest suite (including our new compile_refresh.test) inside
# CI. It keeps the heavy build cache from stage 1.
# -----------------------------------------------------------------------------
FROM openivm-builder AS openivm-tester
WORKDIR /src
CMD ["make", "test"]

# -----------------------------------------------------------------------------
# Stage 3: spark-ext — target-selected JDK + sbt runtime.
# -----------------------------------------------------------------------------
FROM eclipse-temurin:17-jdk-noble@sha256:b73c483fd77e77ad71b25cc146de5172bf9cf34339ccd8aad4996e1878280810 AS jdk17-runtime
FROM eclipse-temurin:21-jdk-noble@sha256:1afee26c547e1aa89f47404854dd09203b8bd35a2be269e16bdfd69c4a2c2b50 AS jdk21-runtime
FROM jdk${JDK_VERSION}-runtime AS spark-ext

ARG OPENIVM_SPARK_TARGET=spark-3.5
ARG JDK_VERSION
ARG SBT_VERSION=1.9.7
ARG SCALA_VERSION=2.12.17
ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu

ENV DEBIAN_FRONTEND=noninteractive
ENV OPENIVM_SPARK_TARGET=${OPENIVM_SPARK_TARGET}
ENV JDK_VERSION=${JDK_VERSION}
ENV SBT_VERSION=${SBT_VERSION}
ENV SCALA_VERSION=${SCALA_VERSION}
ENV OPENIVM_CLI_PATH=/opt/openivm/duckdb
ENV OPENIVM_EXTENSION_PATH=/opt/openivm/openivm.duckdb_extension
ENV OPENIVM_NATIVE_DIR=/opt/openivm

# JVM module-opens block (RESEARCH §10 jvmModuleOpts).
#
# We intentionally do NOT set `JAVA_TOOL_OPTIONS`/`JDK_JAVA_OPTIONS`/`_JAVA_OPTIONS`
# at the container level — every JDK 17 launcher echoes the contents of those
# vars to stderr on startup ("Picked up JAVA_TOOL_OPTIONS: …" / "NOTE: Picked up
# JDK_JAVA_OPTIONS: …"), which sbt routes to its `[error]` channel and clutters
# every test run.
#
# The `--add-opens` / `--add-exports` flags are instead provided silently via:
#   * `spark-ext/.sbtopts` (`-J--add-opens=...`) for the sbt JVM itself
#   * `Settings.scala::jvmModuleOpts` + `Test/javaOptions` for every forked test JVM
#   * `spark.driver.extraJavaOptions` / `spark.executor.extraJavaOptions`  when
#     used with spark-shell / spark-submit (caller's responsibility — see README).

RUN if [ "${UBUNTU_MIRROR}" != "http://archive.ubuntu.com/ubuntu" ]; then \
        sed -i "s|http://archive.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g; s|http://security.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g" \
            /etc/apt/sources.list.d/ubuntu.sources; \
    fi && \
    apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=60 && \
    apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg git make procps less jq && \
    rm -rf /var/lib/apt/lists/*

# Install sbt from sbt-launcher (no apt-repo dependency to keep things hermetic).
RUN mkdir -p /opt/sbt && \
    curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
        -o /tmp/sbt.tgz && \
    tar -xzf /tmp/sbt.tgz -C /opt && \
    rm /tmp/sbt.tgz && \
    ln -sf /opt/sbt/bin/sbt /usr/local/bin/sbt && \
    sbt --version || true   # warm cache on first run; ignore exit code

# Copy openivm artifacts from the builder stage.
COPY --from=native-runtime-check /artifacts/openivm.duckdb_extension /opt/openivm/openivm.duckdb_extension
COPY --from=native-runtime-check /artifacts/duckdb                   /opt/openivm/duckdb
COPY --from=native-runtime-check /artifacts/SHA256SUMS               /opt/openivm/SHA256SUMS

# Sanity check the artifact landed.
RUN ls -l /opt/openivm && /opt/openivm/duckdb --version

# Source mount point (overridden via docker compose to bind /work/spark-ext).
WORKDIR /work/spark-ext

# Pre-cache the sbt launcher dependencies so subsequent sbt runs are fast.
# We do this against an empty project so the first real `sbt compile` doesn't
# pull from Maven Central inside the inner loop.
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash"]
