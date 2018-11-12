from collections import defaultdict
from decimal import Decimal, ROUND_DOWN
import enum
import io
import json
import logging
import operator
from pathlib import Path
import pkg_resources
import sys
from typing import Container, Collection, Mapping, Sequence

import attr
import psutil

from ai.backend.common.utils import readable_size_to_bytes
from .accelerator import AbstractAcceleratorInfo, ProcessorIdType, accelerator_types
from .vendor.linux import libnuma

log = logging.getLogger('ai.backend.agent.resources')


@attr.s(auto_attribs=True, slots=True)
class Share:
    device_id: ProcessorIdType
    share: Decimal


class MountPermission(enum.Enum):
    READ_ONLY = 'ro'
    READ_WRITE = 'rw'


@attr.s(auto_attribs=True, slots=True)
class Mount:
    host_path: Path
    kernel_path: Path
    permission: MountPermission

    def __str__(self):
        return f'{self.host_path}:{self.kernel_path}:{self.permission.value}'

    @classmethod
    def from_str(cls, s):
        hp, kp, perm = s.split(':')
        hp = Path(hp)
        kp = Path(kp)
        perm = MountPermission(perm)
        return cls(hp, kp, perm)


@attr.s(auto_attribs=True, slots=True)
class KernelResourceSpec:
    shares: Mapping[str, Container[Share]]
    memory_limit: int = None
    numa_node: int = None
    cpu_set: Container[int] = attr.Factory(set)
    mounts: Sequence[str] = attr.Factory(list)
    scratch_disk_size: int = None

    reserved_share_types = frozenset(['_cpu', '_mem', '_gpu', '_tpu'])

    def write_to_file(self, file: io.TextIOBase):
        '''
        Write the current resource specification into a file-like object.
        '''
        mega = lambda num_bytes: (
            f'{num_bytes // (2 ** 20)}M'
            if num_bytes > (2 ** 20) else f'{num_bytes}')
        cpu_set_str = ','.join(sorted(map(str, self.cpu_set)))
        file.write(f'NUMA_NODE={self.numa_node}\n')
        file.write(f'CPU_CORES={cpu_set_str}\n')
        file.write(f'MEMORY_LIMIT={mega(self.memory_limit)}\n')
        file.write(f'SCRATCH_SIZE={mega(self.scratch_disk_size)}\n')
        for share_type, shares in self.shares.items():
            if share_type in type(self).reserved_share_types:
                shares_str = str(shares)
            else:
                shares_str = ','.join(
                    f'{dev_id}:{dev_share}'
                    for dev_id, dev_share in shares.items())
            file.write(f'{share_type.upper()}_SHARES={shares_str}\n')
        mounts_str = ','.join(map(str, self.mounts))
        file.write(f'MOUNTS={mounts_str}\n')

    @classmethod
    def read_from_file(cls, file: io.TextIOBase):
        '''
        Read resource specification values from a file-like object.
        '''
        kvpairs = {}
        for line in file:
            key, val = line.strip().split('=', maxsplit=1)
            kvpairs[key] = val
        shares = {}
        for key, val in kvpairs.items():
            if key.endswith('_SHARES'):
                share_type = key[:-7].lower()
                if share_type in cls.reserved_share_types:
                    share_details = Decimal(val)
                else:
                    share_details = {}
                    for entry in val.split(','):
                        dev_id, dev_share = entry.split(':')
                        try:
                            dev_id = int(dev_id)
                        except ValueError:
                            pass
                        dev_share = Decimal(dev_share)
                        share_details[dev_id] = dev_share
                shares[share_type] = share_details
        mounts = [Mount.from_str(m) for m in kvpairs['MOUNTS'].split(',') if m]
        return cls(
            numa_node=int(kvpairs['NUMA_NODE']),
            cpu_set=set(map(int, kvpairs['CPU_CORES'].split(','))),
            memory_limit=readable_size_to_bytes(kvpairs['MEMORY_LIMIT']),
            scratch_disk_size=readable_size_to_bytes(kvpairs['SCRATCH_SIZE']),
            shares=shares,
            mounts=mounts,
        )

    def to_json(self):
        o = attr.asdict(self)
        o['cpu_set'] = list(sorted(o['cpu_set']))
        for dev_type, dev_shares in o['shares'].items():
            if dev_type in type(self).reserved_share_types:
                o['shares'][dev_type] = str(dev_shares)
                continue
            o['shares'][dev_type] = {
                dev_id: str(dev_share)
                for dev_id, dev_share in dev_shares.items()
            }
        o['mounts'] = list(map(str, self.mounts))
        return json.dumps(o)


class CPUAllocMap:

    def __init__(self, limit_cpus=None):
        self.limit_cpus = limit_cpus
        self.core_topo = libnuma.get_core_topology(limit_cpus)
        self.num_cores = len(libnuma.get_available_cores())
        if limit_cpus is not None:
            self.num_cores = min(self.num_cores, len(limit_cpus))
        assert sum(len(node) for node in self.core_topo) == self.num_cores
        self.num_nodes = libnuma.num_nodes()
        self.core_shares = tuple({c: 0 for c in self.core_topo[node]
                                 if limit_cpus is None or c in limit_cpus}
                                 for node in range(self.num_nodes))
        self.alloc_per_node = {n: 0 for n in range(self.num_nodes)
                               if len(self.core_shares[n]) > 0}

    def alloc(self, num_cores):
        '''
        Find a most free set of CPU cores and return a tuple of the NUMA node
        index and the set of integers indicating the found cores.
        This method guarantees that all cores are alloacted within the same
        NUMA node.
        '''
        node, current_alloc = min(
            ((n, alloc) for n, alloc in self.alloc_per_node.items()),
            key=operator.itemgetter(1))
        self.alloc_per_node[node] = current_alloc + num_cores

        shares = self.core_shares[node].copy()
        allocated_cores = set()
        for _ in range(num_cores):
            core, share = min(((core, share) for core, share in shares.items()),
                              key=operator.itemgetter(1))
            allocated_cores.add(core)
            shares[core] = sys.maxsize   # prune allocated one
            self.core_shares[node][core] += 1  # update the original share
        return node, allocated_cores

    def update(self, core_set):
        '''
        Manually add a given core set as if it is allocated by us.
        '''
        any_core = next(iter(core_set))
        node = libnuma.node_of_cpu(any_core)
        self.alloc_per_node[node] += len(core_set)
        for c in core_set:
            self.core_shares[node][c] += 1

    def free(self, core_set):
        '''
        Remove the given set of CPU cores from the allocated shares.
        It assumes that all cores are in the same NUMA node.
        '''
        any_core = next(iter(core_set))
        node = libnuma.node_of_cpu(any_core)
        self.alloc_per_node[node] -= len(core_set)
        for c in core_set:
            self.core_shares[node][c] -= 1


class AcceleratorAllocMap:

    def __init__(self,
                 devices: Collection[AbstractAcceleratorInfo],
                 limit_mask: Container[ProcessorIdType] = None):
        self._quantum = Decimal('.01')
        self.limit_mask = limit_mask
        self.devices = {dev.device_id: dev for dev in devices}
        zero = Decimal('0')
        self.device_shares = {
            p.device_id: zero for p in devices
            if limit_mask is None or p.device_id in limit_mask
        }

    def alloc(self, requested_share: Decimal, node: int = None):
        requested_share = Decimal(requested_share) \
                          .quantize(self._quantum, ROUND_DOWN)
        if node is None:
            # try finding a node, but this also may return "None"
            node = self._find_most_free_node()
        remaining_share = requested_share
        zero = Decimal('0')
        assert requested_share > zero, \
               'You cannot allocate zero share of devices.'
        allocated_shares = defaultdict(lambda: zero)
        try:
            # try filling up beginning from largest free shares
            # TODO: apply advanced scheduling and make it replacible
            while remaining_share > zero:
                p, s = self._find_largest_free_share(node)
                if s >= remaining_share:  # shortcut
                    self.device_shares[p] += remaining_share
                    allocated_shares[p] += remaining_share
                    remaining_share = zero
                elif s == 0:
                    raise RuntimeError('Cannot allocate requested shares '
                                       f'in NUMA node {node}')
                else:  # s < remaining_share
                    self.device_shares[p] += s
                    allocated_shares[p] += s
                    remaining_share -= s
        except RuntimeError:
            # revert back
            for p, s in allocated_shares.items():
                self.device_shares[p] -= s
            raise
        return node, allocated_shares

    def free(self, allocated_shares: Mapping[ProcessorIdType, Decimal]):
        for proc, share in allocated_shares.items():
            self.device_shares[proc] -= share

    def _find_most_free_node(self):
        zero = Decimal('0')
        per_node_allocs = defaultdict(lambda: zero)
        for p, s in self.device_shares.items():
            per_node_allocs[self.devices[p].numa_node] += s
        node, _ = min(
            ((n, alloc) for n, alloc in per_node_allocs.items()),
            key=operator.itemgetter(1))
        return node

    def _find_largest_free_share(self, node: int = None):
        shares = [
            (proc, self.devices[proc].max_share() - share)
            for proc, share in self.device_shares.items()
            if node is None or self.devices[proc].numa_node == node
        ]
        largest_proc_share = max(shares, key=operator.itemgetter(1))
        return largest_proc_share


def bitmask2set(mask):
    bpos = 0
    bset = []
    while mask > 0:
        if (mask & 1) == 1:
            bset.append(bpos)
        mask = (mask >> 1)
        bpos += 1
    return frozenset(bset)


async def detect_slots(etcd, limit_cpus=None, limit_gpus=None):
    '''
    Detect available resource of the system and calculate mem/cpu/gpu slots.
    '''

    mem_bytes = psutil.virtual_memory().total
    num_cores = len(libnuma.get_available_cores())
    if limit_cpus is not None:
        num_cores = min(num_cores, len(limit_cpus))
    slots = {
        'mem': mem_bytes >> 20,  # MiB
        'cpu': num_cores,        # core count
        'gpu': '0.0',
        'tpu': '0.0',
    }
    entry_prefix = 'backendai_accelerator_v10'
    for entrypoint in pkg_resources.iter_entry_points(entry_prefix):
        log.info(f'loading accelerator plugin: {entrypoint.module_name}')
        plugin = entrypoint.load()
        await plugin.init(etcd)
    for accel_type, accel in accelerator_types.items():
        total_share = sum(
            dev.max_share() for dev in accel.list_devices()
            if limit_gpus is None or dev.device_id not in limit_gpus)
        slots[accel.slot_key] = str(total_share)
    log.info(f'Resource slots: {slots!r}')
    return slots
