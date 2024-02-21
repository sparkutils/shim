# shim

Shim is a thin facade over Apache Spark internal APIs smoothing over differences between runtime versions.

Quality and a library it depends on - Frameless - make use of internal "private" Apache Spark APIs, these change between versions both in source and binary incompatible ways.

Moreover, it's not just OSS Spark, vendors are free to backport changes from newer OSS branches, in most cases providing performance or safety features to their users.

Shim aims to alleviate the risk of such changes and provide a faster solution to such changes.

!!! Warn "Avoid 2.4 support"
    This will be removed ASAP and is only introduced during the conversion of Quality to use Shim

## What it is

Shim provides a stable interface over Spark internals used by Quality and Frameless, libraries can use an appropriate public interface shim over the Spark Internal APIs to build against and users can choose which runtime shim to use.

This allows you to focus on your logic and not on variations between runtimes.  At such as stage as the Spark private interface changes radically the public shim interface will get an upgrade to support it.

The 4.x StaticInvoke interface, for example, adds a new default parameter to the case class, this doesn't affect source compatibility but it *does* affect binary compat, the default functions generated by the Scala compiler for the other function lengths can vary, but the Unapply used in matches will definitely break linkage.  The StaticInvoke change requires all libraries to recompile against 4.0, even if it's not published yet, any vendor using this change will stop binary compatibility of libraries using the StaticInvoke private interface.  

Shim treats this as a new runtime version **not** a new compile time dependency and, as such, should not require dependent libraries to publish new versions.

## What it is not

Shim isn't a wrapper around all of the internal apis, it only supports the functionality needed by Quality and Frameless.

It doesn't paper over differences in public apis.

## Naming and Versioning Scheme

The naming scheme is as follows:

    shim_[runtime|compilation]_${runtimeCompatVersion}_${sparkCompatVersion}_${scalaCompatVersion}

The sparkCompatVersion represents the base OSS Spark version and runtimeCompatVersion the actual binary runtime.  As such:

    shim_runtime_14.2.dbr_3.5_2.12

is built using a 3.5 base but specifically targeting a 14.2 Databricks runtime (e.g. for the StaticInvoke change).  Developers therefore use an appropriate OSS runtime and sparkCompatVersion to build against but users may choose other runtime versions.

Non OSS users should use the "compilation" artefacts to build against as provided scope and "runtime" as compile, with the "compilation" artefact higher on the classpath (see below compilation section)

Versions use the following convention:

    major.minor.build

The public compile time api _should_ have stable major and minor numbers (with build likely changing only due to publishing issues), runtimes are _likely_ to have multiple builds.

Build versions _will_ increment across all runtimes.  This means an initial 14.2_dbr runtime may have 0.1.0, then, if a 14.2 specific version upgrade is needed 0.1.1 is taken, a subsequent 14.1 fix will be 0.1.2 etc.

Introduction of a new runtime compat _should_ not force a major.minor increment unless there is a fundamental source incompatibility introduced (e.g. moving of package, demand for a new default param etc.).

Where it is not possible to backport/support newer functionality this will necessarily force a minor version at least.

### Why are there "compilation" runtimeCompatVersions?

In order to build against a number of Databricks runtimes the actual base source version must be used.  These 'compilation' versions like:

    shim_compilation_14.2.dbr_3.5_2.12

should be provided scope and not used for runtime as they include code providing a different Spark internal API than that of the OSS base (in this case 3.5).  The Databricks compilation versions include custom UnaryNode implementations for 10.4 or 9.1s nodePattern usage etc. and are not compatible with OSS runtimes.

OSS compilation jars are provided for easier configuration.

!!! NOTE "Separate Compilation is required for non-OSS only when using changed APIs"
    In practical terms this means Frameless can build against OSS versions and use Databricks runtimes as it does not use classes known to be changed by Databricks (e.g. UnaryNode) (as of 14.2 - this is of course subject to change).

    As only the interfaces for compilation are provided it means, typically, that test suites are not likely to run and that you cannot mix runtimeCompatVersion artefacts.  This does not need to apply to users of the library however.

## How is it achieved

* Forwarding functions for all function usage with differing apis (e.g. UnresolvedFunction names, base Encoder derivation)
* Case classes get arity specific named instances for apply and unapply. (e.g. StaticInvoke / Cast / Add get StaticInvoke5, Cast3 etc. objects )
* Where possible types are simplified (UnresolvedFunction uses String for the name)
* Runtimes can hide Spark interfaces (i.e. hide incompatibilities with the base OSS version)
* Utility traits are introduced that provide default functionality 
* Copying source code from Spark where needed

The public compile time interface usage of arity introduces some verbosity, it is suggested developers use aliases on import to hide this.
 
