Changes
=======

18.12.0 (to be released)
------------------------

- Version numbers now follow year.month releases like Docker.
  We plan to release stable versions on every 3 months (e.g., 18.12, 19.03, ...).

18.12.0a3 (2018-12-21)
----------------------

- Technical release to fix the backend.ai-common dependency version.

18.12.0a2 (2018-12-21)
----------------------

- NEW: Supports user-specified ranges for the service ports published by containers
  via the ``--container-port-range`` CLI argument for firewall-sensitive setups.
  (The default range is 30000-31000) (#90)

- CHANGE: The agent now automatically pulls the image if not available in the host.

- CHANGE: The process monitoring tools will now show prettified process names for
  Backend.AI's daemon processes which exhibit the role and key configurations (e.g.,
  namespace) at a glance.

- Improve support for using custom/private Docker registries.

18.12.0a1 (2018-12-14)
----------------------

- NEW: App service ports!  You can start a compute session and directly connect to a
  service running inside it, such as Jupyter Notebook! (#89)

- Internal refactoring to clean up and fix bugs related to image name references.

- Fix bugs in statistics collection.

- Monitoring tools are separated as plugins.

1.4.0 (2018-09-30)
------------------

- Generalizes accelerator supports

  - Accelerators such as CUDA GPUs can be installed as a separate plugin (#66)

  - Adds support for nvidia-docker v2 (#64)

  - Adds support for allocation of multiple accelerators for one kernel container as
    well as partial shares of each accelerator (#66)

- Revamp the agent restart and kernel initialization processes (#35, #73)

- The view of the agent can be limited to specific CPU cores and GPUs
  using extra CLI arguments: ``--limit-cpus``, ``--limit-gpus`` for
  debugging and performance benchmarks. (#65)

1.3.7 (2018-04-05)
------------------

- Hotfix for handling of dotted image names when they are terminated.

1.3.6 (2018-04-05)
------------------

- Hotfix for handling subdirectories in batch-mode file uploads.

1.3.5 (2018-03-20)
------------------

- Fix vfolder mounts to use the configuration specified in the etcd.
  (No more fixed to "/mnt"!)

1.3.4 (2018-03-19)
------------------

- Fix occasional KeyError when destroying kernels. (#56)

- Deploy a debug log for occasional FileNotFoundError when uploading files
  in the batch mode. (#57)

1.3.3 (2018-03-15)
------------------

- Fix wrong kernel_host sent back to the manager when not overridden.

1.3.2 (2018-03-15)
------------------

- Technical release to fix backend.ai-common depedency version.

1.3.1 (2018-03-14)
------------------

- Technical release to update CI configuration.

1.3.0 (2018-03-08)
------------------

- Fix repeating docker event polling even when there is connection/client-side
  aiohttp errors.

- Upgrade aiohttp to v3.0 release.

- Improve dockerization. (#55)

- Improve inner beauty.

1.2.0 (2018-01-30)
------------------

**NOTICE**

- From this release, the manager and agent versions will go together, which indicates
  the compatibility of them, even when either one has relatively little improvements.

**CHANGES**

- Include the exit code of the last executed in-kernel process when returning
  ``build-finished`` or ``finished`` results in the batch mode.

- Improve logging to support rotating file-based logs.

- Upgrade aiotools to v0.5.2 release.

- Remove the image name prefix when reporting available images. (#51)

- Improve debug-kernel mode to mount host-side kernel runner source into the kernel
  containers so that they use the latest, editable source clone of the kernel runner.

1.1.0 (2018-01-06)
------------------

- Automatically assign the run ID if set None when starting a run.

- Pass environment variables in the start-config to the kernels via
  ``/home/work/.config/environ.txt`` file mounted inside kernels.

- Include the list of kernel images available to the agent when sending
  heartbeats. (#51)

- Remove simplejson from dependencies in favor of the standard library.
  The stdlib has been updated to support all required features and use
  an internal C-based module for performance.

1.0.6 (2017-11-29)
------------------

- Update aioredis to v1.0.0 release.

- Remove "mode" argument from completion RPC calls.

- Fix a bug when terminating overlapped execute streams, which has caused
  indefinite hangs in the client side due to missing "finished" notification.

1.0.5 (2017-11-17)
------------------

- Implement virtual folder mounting (assuming /mnt is already configured)

1.0.4 (2017-11-14)
------------------

- Fix synchronization issues when restarting kernels

- Improve "debug-kernel" mode to use the given kernel name

1.0.3 (2017-11-11)
------------------

- Fix a bug in duplicate-check of our Docker event stream monitoring coroutine

1.0.2 (2017-11-10)
------------------

- Fix automatic mounting of deeplearning-samples Docker volume for ML kernels

- Stabilize statistics collection

- Fix typos

1.0.1 (2017-11-08)
------------------

- Prevent duplicate Docker event generation

- Various bug fixes and improvements (#44, #45, #46, #47)

1.0.0 (2017-10-17)
------------------

- This release is replaced with v1.0.1 due to many bugs.

**CHANGES**

- Rename the package to "Backend.AI" and the import path to ``ai.backend.agent``

- Rewrite interaction with the manager

- Read configuration from etcd shared with the manager

- Add FIFO-style scheduling of overlapped execution requests

- Implement I/O and network statistic collection using sysfs

0.9.14 (2017-08-29)
-------------------

**FIX**

- Fix and improve version reference mechanisms.

- Fix missing import error vanished during hostfix cherrypick

0.9.12 (2017-08-29)
-------------------

**IMPROVEMENTS**

- It now applies the same UID to the spawned containers if they have the "uid-match"
  feature label flag. (backported from develop)

0.9.11 (2017-07-19)
-------------------

**FIX**

- Add missing "sorna-common" dependency and update other requirements.

0.9.10 (2017-07-18)
-------------------

**FIX**

- Fix the wrong version range of an optional depedency package "datadog"

0.9.9 (2017-07-18)
------------------

**IMPROVEMENTS**

- Improve packaging so that setup.py has the source list of dependencies
  whereas requirements.txt has additional/local versions from exotic
  sources.

- Support exception/event logging with Sentry and runtime statistics with Datadog.

0.9.8 (2017-06-30)
------------------

**FIX**

- Fix interactive user inputs in the batch-mode execution.

0.9.7 (2017-06-29)
------------------

**NEW**

- Add support for the batch-mode API with compiled languages such as
  C/C++/Java/Rust.

- Add support for the file upload API for use with the batch-mode API.
  (up to 20 files per request and 1 MiB per each file)

**CHANGES**

- Only files stored in "/home/work.output" directories of kernel containers
  are auto-uploaded to S3 as downloadable files, as now we rely on our
  dedicated multi-media output interfaces to show plots and other graphics.
  Previously, all non-hidden files in "/home/work" were uploaded.

0.9.6 (2017-04-12)
------------------

- Fix a regression in console output streaming.

0.9.5 (2017-04-07)
------------------

- Add PyTorch support.

- Upgrade aiohttp to v2 and relevant dependencies as well.

0.9.4 (2017-03-19)
------------------

- Update missing long_description.

0.9.3 (2017-03-19)
------------------

- Improve packaging: auto-converted README.md as long description and unified
  requirements.txt and setup.py dependencies.

0.9.2 (2017-03-14)
------------------

- Fix sorna-common requirement version.

0.9.1 (2017-03-14)
------------------

**CHANGES**

- Separate console output formats for API v1 and v2.

- Deprecate unused matching option for execution API.

- Remove control messages in API responses.

0.9.0 (2017-02-27)
------------------

**NEW**

- PUSH/PULL-based kernel interaction protocol to support streaming outputs.
  This enables interactive input functions and streaming outputs for long-running codes,
  and also makes kernel execution more resilient to network failures.
  (ZeroMQ's REQ/REP sockets break the system if any messages get dropped)

0.8.2 (2017-01-16)
------------------

**FIXES**

- Fix a typo that generates errors during GPU kernel initialization.

- Fix regression of '--agent-ip-override' cli option.

0.8.1 (2017-01-10)
------------------

- Minor internal polishing release.

0.8.0 (2017-01-10)
------------------

**CHANGES**

- Bump version to 0.8 to match with sorna-manager and sorna-client.

**FIXES**

- Fix events lost by HTTP connection timeouts when using ``docker.events.run()`` from
  aiodocker.  (It is due to default 5-minute timeout set by aiohttp)

- Correct task cancellation

0.7.5 (2016-12-01)
------------------

**CHANGES**

- Add new aliases for "git" kernel: "git-shell" and "shell"

0.7.4 (2016-12-01)
------------------

**CHANGES**

- Now it uses `aiodocker`_ instead of `docker-py`_ to
  prevent timeouts with many concurrent requests.

  NOTE: You need to run ``pip install -r requirements.txt`` to install the
        non-pip (GitHub) version of aiodocker correctly, before running
        ``pip install sorna-agent``.

**FIXES**

- Fix corner-case exceptions in statistics/heartbeats.

.. _aiodocker: https://github.com/achimnol/aiodocker

.. _dockerpy: https://github.com/docker/docker-py

0.7.3 (2016-11-30)
------------------

**CHANGES**

- Increase docker API timeouts.

**FIXES**

- Fix heartbeats stop working after kernel/agent timeouts.

- Fix exception logging in the main server loop.

0.7.2 (2016-11-28)
------------------

**FIXES**

- Hotfix for missing dependency: coloredlogs

0.7.1 (2016-11-27)
------------------

**NEW**

- ``--agent-ip-override`` CLI option to override the IP address of agent
  reported to the manager.

0.7.0 (2016-11-25)
------------------

**NEW**

- Add support for kernel restarts.
  Restarting preserves kernel metadata and its ID, but removes and recreates
  the working volume and the container itself.

- Add ``--debug`` option to the CLI command.

0.6.0 (2016-11-14)
------------------

**NEW**

- Add support for GPU-enabled kernels (using `nvidia-docker plugin`_).
  The kernel images must be built upon nvidia-docker's base Ubuntu images and
  have the label "io.sorna.nvidia.enabled" set ``yes``.

**CHANGES**

- Change the agent to add "lablup/" prefix when creating containers from
  kernel image names, to ease setup and running using the public docker
  repository.  (e.g., "lablup/kernel-python3" instead of "kernel-python3")

- Change the prefix of kernel image labels from "com.lablup.sorna." to
  "io.sorna." for simplicity.

- Increase the default idle timeout to 30 minutes for offline tutorial/workshops.

- Limit the CPU cores available in kernel containers.
  It uses an optional "io.sorna.maxcores" label (default is 1 when not
  specified) to determine the requested number of CPU cores in kernels, with a
  hard limit of 4.

  NOTE: You will still see the full count of CPU cores of the underlying
  system when running ``os.cpu_count()``, ``multiprocessing.cpu_count()`` or
  ``os.sysconf("SC_NPROCESSORS_ONLN")`` because the limit is enforced by the CPU
  affinity mask.  To get the correct result, try
  ``len(os.sched_getaffinity(os.getpid()))``.

.. _nvidia-docker plugin: https://github.com/NVIDIA/nvidia-docker


0.5.0 (2016-11-01)
------------------

**NEW**

- First public release.


<!-- vim: set et: -->
