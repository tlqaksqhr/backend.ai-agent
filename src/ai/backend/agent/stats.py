'''
A module to collect various performance metrics of Docker containers.

Reference: https://www.datadoghq.com/blog/how-to-collect-docker-metrics/
'''

import asyncio
import argparse
from contextlib import closing, contextmanager
from ctypes import CDLL, get_errno
from dataclasses import asdict, dataclass, field
import functools
import logging
import os
from pathlib import Path
import sys
import time

import aiohttp
from aiodocker.docker import Docker, DockerContainer
from aiodocker.exceptions import DockerError
import aiotools
from setproctitle import setproctitle
import zmq
import zmq.asyncio

from ai.backend.common import msgpack
from ai.backend.common.utils import nmget
from ai.backend.common.logging import Logger, BraceStyleAdapter
from ai.backend.common.identity import is_containerized

__all__ = (
    'ContainerStat',
    'StatCollectorState',
    'check_cgroup_available',
    'get_preferred_stat_type',
    'spawn_stat_collector',
    'numeric_list', 'read_sysfs',
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.agent.stats'))

CLONE_NEWNET = 0x40000000


def _errcheck(ret, func, args):
    if ret == -1:
        e = get_errno()
        raise OSError(e, os.strerror(e))


def check_cgroup_available():
    '''
    Check if the host OS provides cgroups.
    '''
    return (not is_containerized() and sys.platform.startswith('linux'))


def get_preferred_stat_type():
    '''
    Returns the most preferred statistics collector type for the host OS.
    '''
    if check_cgroup_available():
        return 'cgroup'
    return 'api'


@dataclass(frozen=False)
class ContainerStat:
    precpu_used: int = 0
    cpu_used: int = 0
    precpu_system_used: int = 0
    cpu_system_used: int = 0
    mem_max_bytes: int = 0
    mem_cur_bytes: int = 0
    net_rx_bytes: int = 0
    net_tx_bytes: int = 0
    io_read_bytes: int = 0
    io_write_bytes: int = 0
    io_max_scratch_size: int = 0
    io_cur_scratch_size: int = 0

    def update(self, stat: 'ContainerStat'):
        if stat is None:
            return
        self.precpu_used = self.cpu_used
        self.cpu_used = stat.cpu_used
        self.precpu_system_used = self.cpu_system_used
        self.cpu_system_used = stat.cpu_system_used
        self.mem_max_bytes = stat.mem_max_bytes
        self.mem_cur_bytes = stat.mem_cur_bytes
        self.net_rx_bytes = max(self.net_rx_bytes, stat.net_rx_bytes)
        self.net_tx_bytes = max(self.net_tx_bytes, stat.net_tx_bytes)
        self.io_read_bytes = max(self.io_read_bytes, stat.io_read_bytes)
        self.io_write_bytes = max(self.io_write_bytes, stat.io_write_bytes)
        self.io_max_scratch_size = max(self.io_max_scratch_size,
                                       stat.io_cur_scratch_size)
        self.io_cur_scratch_size = stat.io_cur_scratch_size


@dataclass(frozen=False)
class StatCollectorState:
    kernel_id: str
    last_stat: ContainerStat = None
    terminated: asyncio.Event = field(default_factory=lambda: asyncio.Event())


async def collect_agent_live_stats(agent):
    """Store agent live stats in redis stats server.
    """
    from .server import stat_cache_lifespan
    num_cores = agent.container_cpu_map.num_cores
    precpu_used = cpu_used = mem_cur_bytes = 0
    precpu_sys_used = cpu_sys_used = 0
    for cid, cstate in agent.stats.items():
        if not cstate.terminated.is_set() and cstate.last_stat is not None:
            precpu_used += float(cstate.last_stat['precpu_used'])
            cpu_used += float(cstate.last_stat['cpu_used'])
            precpu_sys_used += float(cstate.last_stat['precpu_system_used'])
            cpu_sys_used += float(cstate.last_stat['cpu_system_used'])
            mem_cur_bytes += int(cstate.last_stat['mem_cur_bytes'])

    # CPU usage calculation ref: https://bit.ly/2rrfrFF
    cpu_delta = cpu_used - precpu_used
    system_delta = cpu_sys_used - precpu_sys_used
    cpu_pct = 0
    if system_delta > 0 and cpu_delta > 0:
        cpu_pct = (cpu_delta / system_delta) * num_cores * 100

    agent_live_info = {
        'cpu_pct': round(cpu_pct, 1),
        'mem_cur_bytes': mem_cur_bytes,
    }
    pipe = agent.redis_stat_pool.pipeline()
    pipe.hmset_dict(agent.config.instance_id, agent_live_info)
    pipe.expire(agent.config.instance_id, stat_cache_lifespan)
    await pipe.execute()


@aiotools.actxmgr
async def spawn_stat_collector(stat_addr, stat_type, cid, *,
                               exec_opts=None):
    # Spawn high-perf stats collector process for Linux native setups.
    # NOTE: We don't have to keep track of this process,
    #       as they will self-terminate when the container terminates.
    if exec_opts is None:
        exec_opts = {}

    context = zmq.asyncio.Context()
    ipc_base_path = Path('/tmp/backend.ai/ipc')
    ipc_base_path.mkdir(parents=True, exist_ok=True)

    proc = await asyncio.create_subprocess_exec(*[
        'python', '-m', 'ai.backend.agent.stats',
        stat_addr, cid, '--type', stat_type,
    ], **exec_opts)

    signal_path = 'ipc://' + str(ipc_base_path / f'stat-start-{proc.pid}.sock')
    signal_sock = context.socket(zmq.PAIR)
    signal_sock.connect(signal_path)
    try:
        await signal_sock.recv_multipart()
        yield proc
    finally:
        await signal_sock.send_multipart([b''])
        signal_sock.close()
        context.term()


def _collect_stats_sysfs(container_id):
    cpu_prefix = f'/sys/fs/cgroup/cpuacct/docker/{container_id}/'
    mem_prefix = f'/sys/fs/cgroup/memory/docker/{container_id}/'
    io_prefix = f'/sys/fs/cgroup/blkio/docker/{container_id}/'

    try:
        cpu_used = read_sysfs(cpu_prefix + 'cpuacct.usage') / 1e6
        cpu_system_used = read_sysfs('/sys/fs/cgroup/cpuacct/cpuacct.usage') / 1e6
        mem_max_bytes = read_sysfs(mem_prefix + 'memory.max_usage_in_bytes')
        mem_cur_bytes = read_sysfs(mem_prefix + 'memory.usage_in_bytes')

        io_stats = Path(io_prefix + 'blkio.throttle.io_service_bytes').read_text()
        # example data:
        #   8:0 Read 13918208
        #   8:0 Write 0
        #   8:0 Sync 0
        #   8:0 Async 13918208
        #   8:0 Total 13918208
        #   Total 13918208
        io_read_bytes = 0
        io_write_bytes = 0
        for line in io_stats.splitlines():
            if line.startswith('Total '):
                continue
            dev, op, nbytes = line.strip().split()
            if op == 'Read':
                io_read_bytes += int(nbytes)
            elif op == 'Write':
                io_write_bytes += int(nbytes)
        io_max_scratch_size = 0
        io_cur_scratch_size = 0

        net_dev_stats = Path('/proc/net/dev').read_text()
        # example data:
        #   Inter-|   Receive                                                |  Transmit                                                  # noqa: E501
        #    face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed    # noqa: E501
        #     eth0:     1296     16    0    0    0     0          0         0      816      10    0    0    0     0       0          0    # noqa: E501
        #       lo:        0      0    0    0    0     0          0         0        0       0    0    0    0     0       0          0    # noqa: E501
        net_rx_bytes = 0
        net_tx_bytes = 0
        for line in net_dev_stats.splitlines():
            if '|' in line:
                continue
            data = line.strip().split()
            if data[0].startswith('eth'):
                net_rx_bytes += int(data[1])
                net_tx_bytes += int(data[9])
    except IOError as e:
        short_cid = container_id[:7]
        log.warning('cannot read stats: '
                    f'sysfs unreadable for container {short_cid}!'
                    f'\n{e!r}')
        return None

    return ContainerStat(
        0,  # precpu_used calculated automatically
        cpu_used,
        0,  # precpu_system_used calculated autmatically
        cpu_system_used,
        mem_max_bytes,
        mem_cur_bytes,
        net_rx_bytes,
        net_tx_bytes,
        io_read_bytes,
        io_write_bytes,
        io_max_scratch_size,
        io_cur_scratch_size,
    )


async def _collect_stats_api(container):
    try:
        ret = await container.stats(stream=False)
    except (DockerError, aiohttp.ClientResponseError):
        short_cid = container._id[:7]
        log.warning(f'cannot read stats: Docker stats API error for {short_cid}.')
        return None
    else:
        # API returned successfully but actually the result may be empty!
        if ret is None:
            return None
        if ret['preread'].startswith('0001-01-01'):
            return None
        cpu_used = nmget(ret, 'cpu_stats.cpu_usage.total_usage', 0) / 1e6
        cpu_system_used = nmget(ret, 'cpu_stats.system_cpu_usage', 0) / 1e6
        mem_max_bytes = nmget(ret, 'memory_stats.max_usage', 0)
        mem_cur_bytes = nmget(ret, 'memory_stats.usage', 0)

        io_read_bytes = 0
        io_write_bytes = 0
        for item in nmget(ret, 'blkio_stats.io_service_bytes_recursive', []):
            if item['op'] == 'Read':
                io_read_bytes += item['value']
            elif item['op'] == 'Write':
                io_write_bytes += item['value']
        io_max_scratch_size = 0
        io_cur_scratch_size = 0

        net_rx_bytes = 0
        net_tx_bytes = 0
        for dev in nmget(ret, 'networks', {}).values():
            net_rx_bytes += dev['rx_bytes']
            net_tx_bytes += dev['tx_bytes']
    return ContainerStat(
        0,  # precpu_used calculated automatically
        cpu_used,
        0,  # precpu_system_used calculated autmatically
        cpu_system_used,
        mem_max_bytes,
        mem_cur_bytes,
        net_rx_bytes,
        net_tx_bytes,
        io_read_bytes,
        io_write_bytes,
        io_max_scratch_size,
        io_cur_scratch_size,
    )


async def collect_stats(containers):
    if check_cgroup_available():
        results = tuple(_collect_stats_sysfs(c._id) for c in containers)
    else:
        tasks = []
        for c in containers:
            tasks.append(asyncio.ensure_future(_collect_stats_api(c)))
        results = await asyncio.gather(*tasks)
    return results


def numeric_list(s):
    return [int(p) for p in s.split()]


def read_sysfs(path, type_=int, default_val=0):
    return type_(Path(path).read_text().strip())


@contextmanager
def join_cgroup_and_namespace(cid, initial_stat, send_stat, signal_sock):
    libc = CDLL('libc.so.6', use_errno=True)
    libc.setns.errcheck = _errcheck
    mypid = os.getpid()

    # The list of monitored cgroup resource types
    cgroups = ['memory', 'cpuacct', 'blkio', 'net_cls']

    try:
        # Create the cgroups and change my membership.
        # The reason for creating cgroups first is to keep them alive
        # after the Docker has killed the container.
        for cgroup in cgroups:
            cg_path = Path(f'/sys/fs/cgroup/{cgroup}/docker/{cid}')
            cg_path.mkdir(parents=True, exist_ok=True)
            cgtasks_path = Path(f'/sys/fs/cgroup/{cgroup}/docker/{cid}/cgroup.procs')
            cgtasks_path.write_text(str(mypid))
    except PermissionError:
        print('Cannot write cgroup filesystem due to permission error!',
              file=sys.stderr)
        sys.exit(1)

    # Notify the agent to start the container.
    signal_sock.send_multipart([b''])

    # Wait for the container to be started.
    signal_sock.recv_multipart()

    try:
        procs_path = Path(f'/sys/fs/cgroup/net_cls/docker/{cid}/cgroup.procs')
        pids = procs_path.read_text()
        pids = set(int(p) for p in pids.split())
    except PermissionError:
        print('Cannot read cgroup filesystem due to permission error!',
              file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print([p.name for p in Path('/sys/fs/cgroup/memory/docker').iterdir()])
        print('Cannot read cgroup filesystem.\n'
              'The container did not start or may have already terminated.',
              file=sys.stderr)
        send_stat({
            'cid': args.cid,
            'status': 'terminated',
            'data': asdict(initial_stat),
        })
        sys.exit(0)

    try:
        # Get non-myself pid from the current running processes.
        pids.discard(mypid)
        first_pid = pids.pop()
        # Enter the container's network namespace so that we can see the exact "eth0"
        # (which is actually "vethXXXX" in the host namespace)
        with open(f'/proc/{first_pid}/ns/net', 'r') as f:
            libc.setns(f.fileno(), CLONE_NEWNET)
    except PermissionError:
        print('This process must be started with the root privilege or have explicit'
              'CAP_SYS_ADMIN, CAP_DAC_OVERRIDE, and CAP_SYS_PTRACE capabilities.',
              file=sys.stderr)
        sys.exit(1)
    except (FileNotFoundError, KeyError):
        print('The container has already terminated.', file=sys.stderr)
        send_stat({
            'cid': args.cid,
            'status': 'terminated',
            'data': asdict(initial_stat),
        })
        sys.exit(0)

    try:
        yield
    finally:
        # Move to the parent cgroup and self-remove the container cgroup
        for cgroup in cgroups:
            Path(f'/sys/fs/cgroup/{cgroup}/cgroup.procs').write_text(str(mypid))
            try:
                os.rmdir(f'/sys/fs/cgroup/{cgroup}/docker/{cid}')
            except OSError:
                pass


def is_cgroup_running(cid):
    try:
        pids = Path(f'/sys/fs/cgroup/net_cls/docker/{cid}/cgroup.procs').read_text()
        pids = numeric_list(pids)
    except IOError:
        return False
    else:
        return (len(pids) > 1)


def main(args):
    context = zmq.Context.instance()
    mypid = os.getpid()

    ipc_base_path = Path('/tmp/backend.ai/ipc')
    signal_path = str(ipc_base_path / f'stat-start-{mypid}.sock')
    log.debug('creating signal socket at {}', signal_path)
    signal_sock = context.socket(zmq.PAIR)
    signal_sock.bind('ipc://' + signal_path)
    try:

        stats_sock = context.socket(zmq.PUSH)
        stats_sock.setsockopt(zmq.LINGER, 2000)
        stats_sock.connect(args.sockaddr)
        send_stat = functools.partial(
            stats_sock.send_serialized,
            serialize=lambda v: [msgpack.packb(v)])
        stat = ContainerStat()
        log.info('started statistics collection for {}', args.cid)

        if args.type == 'cgroup':
            with closing(stats_sock), join_cgroup_and_namespace(args.cid, stat,
                                                                send_stat,
                                                                signal_sock):
                # Agent notification is done inside join_cgroup_and_namespace
                while True:
                    new_stat = _collect_stats_sysfs(args.cid)
                    stat.update(new_stat)
                    msg = {
                        'cid': args.cid,
                        'data': asdict(stat),
                    }
                    if is_cgroup_running(args.cid) and new_stat is not None:
                        msg['status'] = 'running'
                        send_stat(msg)
                    else:
                        msg['status'] = 'terminated'
                        send_stat(msg)
                        break
                    time.sleep(1.0)
        elif args.type == 'api':
            loop = asyncio.get_event_loop()
            docker = Docker()
            with closing(stats_sock), closing(loop):
                container = DockerContainer(docker, id=args.cid)
                # Notify the agent to start the container.
                signal_sock.send_multipart([b''])
                # Wait for the container to be actually started.
                signal_sock.recv_multipart()
                while True:
                    new_stat = loop.run_until_complete(_collect_stats_api(container))
                    stat.update(new_stat)
                    msg = {
                        'cid': args.cid,
                        'data': asdict(stat),
                    }
                    if new_stat is not None:
                        msg['status'] = 'running'
                        send_stat(msg)
                    else:
                        msg['status'] = 'terminated'
                        send_stat(msg)
                        break
                    time.sleep(1.0)
                loop.run_until_complete(docker.close())

    except (KeyboardInterrupt, SystemExit):
        sys.exit(1)
    else:
        sys.exit(0)
    finally:
        signal_sock.close()
        os.unlink(signal_path)
        log.info('terminated statistics collection for {}', args.cid)


if __name__ == '__main__':
    # The entry point for stat collector daemon
    parser = argparse.ArgumentParser()
    parser.add_argument('sockaddr', type=str)
    parser.add_argument('cid', type=str)
    parser.add_argument('-t', '--type', choices=['cgroup', 'api'],
                        default='cgroup')
    args = parser.parse_args()
    setproctitle(f'backend.ai: stat-collector {args.cid[:7]}')

    log_config = argparse.Namespace()
    log_config.log_file = None
    log_config.debug = False
    logger = Logger(log_config)
    with logger:
        main(args)
