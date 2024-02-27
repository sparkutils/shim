---
tags:
- basic
- getting started
- beginner
---

In short, you should generally be able to use the OSS shim_runtime jars to build against and use the appropriate DBR version to run against - unless you are using internal apis that Databricks has changed then you need the shim_compilation approach (see [here](index.md#developing-a-library-against-internal-apis-changed-by-databricks) for details).

The shims are currently tested under Quality tests (which tests most, but not all Frameless encoding) and, as such, there is currently no direct tests for the shims themselves (this is tracked under #4)
