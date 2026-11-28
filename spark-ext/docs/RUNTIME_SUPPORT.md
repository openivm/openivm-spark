# Runtime support

| Target      | Spark | Delta | Scala   | Java | Maven artifact                |
| ----------- | ----- | ----- | ------- | ---- | ----------------------------- |
| `spark-3.5` | 3.5.1 | 3.2.0 | 2.12.17 | 17   | `ivmextension-spark-3.5_2.12` |
| `spark-4.1` | 4.1.0 | 4.2.0 | 2.13.17 | 21   | `ivmextension-spark-4.1_2.13` |

Spark 3.5 remains the default for every developer command that omits `--target`.

Assembly jars bundle a Linux x86-64 native compiler built and smoke-tested on
Ubuntu 22.04 with its system glibc and libstdc++.

Use the pinned, prebuilt devcontainer rather than host-installed Java, Scala,
Spark or Delta; see [development](DEVELOPMENT.md) and [activation](ACTIVATION.md).
