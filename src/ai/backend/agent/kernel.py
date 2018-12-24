import asyncio
import codecs
from collections import OrderedDict
from dataclasses import dataclass
import io
import json
import logging
import time
import secrets

from async_timeout import timeout
import aiozmq
import msgpack
import zmq

from ai.backend.common.utils import StringSetFlag
from ai.backend.common.logging import BraceStyleAdapter

log = BraceStyleAdapter(logging.getLogger(__name__))

# msg types visible to the API client.
# (excluding control signals such as 'finished' and 'waiting-input'
# since they are passed as separate status field.)
outgoing_msg_types = {'stdout', 'stderr', 'media', 'html', 'log', 'completion'}


class KernelFeatures(StringSetFlag):
    UID_MATCH = 'uid-match'
    USER_INPUT = 'user-input'
    BATCH_MODE = 'batch'
    QUERY_MODE = 'query'
    TTY_MODE = 'tty'


class ClientFeatures(StringSetFlag):
    INPUT = 'input'
    CONTINUATION = 'continuation'


class RunEvent(Exception):

    def __init__(self, data=None):
        super().__init__()
        self.data = data


class InputRequestPending(RunEvent):
    pass


class CleanFinished(RunEvent):
    pass


class BuildFinished(RunEvent):
    pass


class RunFinished(RunEvent):
    pass


class ExecTimeout(RunEvent):
    pass


@dataclass
class ResultRecord:
    msg_type: str = None
    data: str = None


class KernelRunner:

    def __init__(self, kernel_id,
                 kernel_host, repl_in_port, repl_out_port,
                 exec_timeout, client_features=None):
        self.started_at = None
        self.finished_at = None
        self.kernel_id = kernel_id
        self.kernel_host = kernel_host
        self.repl_in_port = repl_in_port
        self.repl_out_port = repl_out_port
        self.input_stream = None
        self.output_stream = None
        assert exec_timeout >= 0
        self.exec_timeout = exec_timeout
        self.max_record_size = 10485760  # 10 MBytes
        self.completion_queue = asyncio.Queue(maxsize=128)
        self.service_queue = asyncio.Queue(maxsize=128)
        self.read_task = None
        self.client_features = client_features or set()

        self.output_queue = None
        self.pending_queues = OrderedDict()
        self.current_run_id = None

    async def start(self):
        self.started_at = time.monotonic()

        self.input_stream = await aiozmq.create_zmq_stream(
            zmq.PUSH, connect=f'tcp://{self.kernel_host}:{self.repl_in_port}')
        self.input_stream.transport.setsockopt(zmq.LINGER, 50)

        self.output_stream = await aiozmq.create_zmq_stream(
            zmq.PULL, connect=f'tcp://{self.kernel_host}:{self.repl_out_port}')
        self.output_stream.transport.setsockopt(zmq.LINGER, 50)

        self.read_task = asyncio.ensure_future(self.read_output())
        if self.exec_timeout > 0:
            self.watchdog_task = asyncio.ensure_future(self.watchdog())
        else:
            self.watchdog_task = None

    async def close(self):
        if self.watchdog_task and not self.watchdog_task.done():
            self.watchdog_task.cancel()
            await self.watchdog_task
        if (self.input_stream and not self.input_stream.at_closing() and
            self.input_stream.transport):
            # only when really closable...
            self.input_stream.close()
        if (self.output_stream and not self.output_stream.at_closing() and
            self.output_stream.transport):
            # only when really closable...
            self.output_stream.close()
        if self.read_task and not self.read_task.done():
            self.read_task.cancel()
            await self.read_task
            self.read_task = None

    async def feed_batch(self, opts):
        clean_cmd = opts.get('clean', '')
        if clean_cmd is None:
            clean_cmd = ''
        self.input_stream.write([
            b'clean',
            clean_cmd.encode('utf8'),
        ])
        build_cmd = opts.get('build', '')
        if build_cmd is None:
            build_cmd = ''
        self.input_stream.write([
            b'build',
            build_cmd.encode('utf8'),
        ])
        exec_cmd = opts.get('exec', '')
        if exec_cmd is None:
            exec_cmd = ''
        self.input_stream.write([
            b'exec',
            exec_cmd.encode('utf8'),
        ])

    async def feed_code(self, text):
        self.input_stream.write([b'code', text.encode('utf8')])

    async def feed_input(self, text):
        self.input_stream.write([b'input', text.encode('utf8')])

    async def feed_interrupt(self):
        self.input_stream.write([b'interrupt', b''])

    async def feed_and_get_completion(self, code_text, opts):
        payload = {
            'code': code_text,
        }
        payload.update(opts)
        self.input_stream.write([
            b'complete',
            json.dumps(payload).encode('utf8'),
        ])
        try:
            result = await self.completion_queue.get()
            self.completion_queue.task_done()
            return json.loads(result)
        except asyncio.CancelledError:
            return []

    async def feed_start_service(self, service_info):
        self.input_stream.write([
            b'start-service',
            json.dumps(service_info).encode('utf8'),
        ])
        try:
            result = await self.service_queue.get()
            self.service_queue.task_done()
            return json.loads(result)
        except asyncio.CancelledError:
            return {'status': 'failed', 'error': 'cancelled'}

    async def watchdog(self):
        try:
            await asyncio.sleep(self.exec_timeout)
            if self.output_queue is not None:
                # TODO: what to do if None?
                self.output_queue.put(ResultRecord('exec-timeout', None))
        except asyncio.CancelledError:
            pass

    @staticmethod
    def aggregate_console(result, records, api_ver):

        if api_ver == 1:

            stdout_items = []
            stderr_items = []
            media_items = []
            html_items = []

            for rec in records:
                if rec.msg_type == 'stdout':
                    stdout_items.append(rec.data)
                elif rec.msg_type == 'stderr':
                    stderr_items.append(rec.data)
                elif rec.msg_type == 'media':
                    o = json.loads(rec.data)
                    media_items.append((o['type'], o['data']))
                elif rec.msg_type == 'html':
                    html_items.append(rec.data)

            result['stdout'] = ''.join(stdout_items)
            result['stderr'] = ''.join(stderr_items)
            result['media'] = media_items
            result['html'] = html_items

        elif api_ver in (2, 3):

            console_items = []
            last_stdout = io.StringIO()
            last_stderr = io.StringIO()

            for rec in records:

                if last_stdout.tell() and rec.msg_type != 'stdout':
                    console_items.append(('stdout', last_stdout.getvalue()))
                    last_stdout.seek(0)
                    last_stdout.truncate(0)
                if last_stderr.tell() and rec.msg_type != 'stderr':
                    console_items.append(('stderr', last_stderr.getvalue()))
                    last_stderr.seek(0)
                    last_stderr.truncate(0)

                if rec.msg_type == 'stdout':
                    last_stdout.write(rec.data)
                elif rec.msg_type == 'stderr':
                    last_stderr.write(rec.data)
                elif rec.msg_type == 'media':
                    o = json.loads(rec.data)
                    console_items.append((rec.msg_type, (o['type'], o['data'])))
                elif rec.msg_type in outgoing_msg_types:
                    console_items.append((rec.msg_type, rec.data))

            if last_stdout.tell():
                console_items.append(('stdout', last_stdout.getvalue()))
            if last_stderr.tell():
                console_items.append(('stderr', last_stderr.getvalue()))

            result['console'] = console_items
            last_stdout.close()
            last_stderr.close()

        else:
            raise AssertionError('Unrecognized API version')

    async def get_next_result(self, api_ver=2, flush_timeout=2.0):
        # Context: per API request
        has_continuation = ClientFeatures.CONTINUATION in self.client_features
        try:
            records = []
            with timeout(flush_timeout if has_continuation else None):
                while True:
                    rec = await self.output_queue.get()
                    if rec.msg_type in outgoing_msg_types:
                        records.append(rec)
                    self.output_queue.task_done()
                    if rec.msg_type == 'finished':
                        data = json.loads(rec.data) if rec.data else {}
                        raise RunFinished(data)
                    elif rec.msg_type == 'clean-finished':
                        data = json.loads(rec.data) if rec.data else {}
                        raise CleanFinished(data)
                    elif rec.msg_type == 'build-finished':
                        data = json.loads(rec.data) if rec.data else {}
                        raise BuildFinished(data)
                    elif rec.msg_type == 'waiting-input':
                        opts = json.loads(rec.data) if rec.data else {}
                        raise InputRequestPending(opts)
                    elif rec.msg_type == 'exec-timeout':
                        raise ExecTimeout
        except asyncio.TimeoutError:
            result = {
                'runId': self.current_run_id,
                'status': 'continued',
                'exitCode': None,
                'options': None,
            }
            type(self).aggregate_console(result, records, api_ver)
            self.resume_output_queue()
            return result
        except CleanFinished as e:
            result = {
                'runId': self.current_run_id,
                'status': 'clean-finished',
                'exitCode': e.data.get('exitCode'),
                'options': None,
            }
            type(self).aggregate_console(result, records, api_ver)
            self.resume_output_queue()
            return result
        except BuildFinished as e:
            result = {
                'runId': self.current_run_id,
                'status': 'build-finished',
                'exitCode': e.data.get('exitCode'),
                'options': None,
            }
            type(self).aggregate_console(result, records, api_ver)
            self.resume_output_queue()
            return result
        except RunFinished as e:
            result = {
                'runId': self.current_run_id,
                'status': 'finished',
                'exitCode': e.data.get('exitCode'),
                'options': None,
            }
            type(self).aggregate_console(result, records, api_ver)
            self.next_output_queue()
            return result
        except ExecTimeout:
            result = {
                'runId': self.current_run_id,
                'status': 'exec-timeout',
                'exitCode': None,
                'options': None,
            }
            log.warning('Execution timeout detected on kernel '
                        f'{self.kernel_id}')
            type(self).aggregate_console(result, records, api_ver)
            self.next_output_queue()
            return result
        except InputRequestPending as e:
            result = {
                'runId': self.current_run_id,
                'status': 'waiting-input',
                'exitCode': None,
                'options': e.data,
            }
            type(self).aggregate_console(result, records, api_ver)
            self.resume_output_queue()
            return result
        except asyncio.CancelledError:
            self.resume_output_queue()
            raise
        except Exception:
            log.exception('unexpected error')
            raise

    async def attach_output_queue(self, run_id):
        # Context: per API request
        if run_id is None:
            run_id = secrets.token_hex(16)
        assert run_id is not None
        if run_id not in self.pending_queues:
            q = asyncio.Queue(maxsize=4096)
            activated = asyncio.Event()
            self.pending_queues[run_id] = (activated, q)
        else:
            activated, q = self.pending_queues[run_id]
        if self.output_queue is None:
            self.output_queue = q
        else:
            if self.current_run_id == run_id:
                # No need to wait if we are continuing.
                pass
            else:
                # If there is an outstanding ongoning execution,
                # wait until it has "finished".
                await activated.wait()
                activated.clear()
        self.current_run_id = run_id
        assert self.output_queue is q

    def resume_output_queue(self):
        '''
        Use this to conclude get_next_result() when the execution should be
        continued from the client.

        At that time, we need to reuse the current run ID and its output queue.
        We don't change self.output_queue here so that we can continue to read
        outputs while the client sends the continuation request.
        '''
        self.pending_queues.move_to_end(self.current_run_id, last=False)

    def next_output_queue(self):
        '''
        Use this to conclude get_next_result() when we have finished a "run".
        '''
        assert self.current_run_id is not None
        self.pending_queues.pop(self.current_run_id, None)
        self.current_run_id = None
        if len(self.pending_queues) > 0:
            # Make the next waiting API request handler to proceed.
            _, (activated, q) = self.pending_queues.popitem(last=False)
            self.output_queue = q
            activated.set()
        else:
            # If there is no pending request, just ignore all outputs
            # from the kernel.
            self.output_queue = None

    async def read_output(self):
        # We should use incremental decoder because some kernels may
        # send us incomplete UTF-8 byte sequences (e.g., Julia).
        decoders = (
            codecs.getincrementaldecoder('utf8')(errors='replace'),
            codecs.getincrementaldecoder('utf8')(errors='replace'),
        )
        while True:
            try:
                msg_type, msg_data = await self.output_stream.read()
                # TODO: test if save-load runner states is working
                #       by printing received messages here
                if len(msg_data) > self.max_record_size:
                    msg_data = msg_data[:self.max_record_size]
                try:
                    if msg_type == b'status':
                        msgpack.unpackb(msg_data, encoding='utf8')
                        # TODO: not implemented yet
                    elif msg_type == b'completion':
                        # As completion is processed asynchronously
                        # to the main code execution, we directly
                        # put the result into a separate queue.
                        await self.completion_queue.put(msg_data)
                    elif msg_type == b'service-result':
                        await self.service_queue.put(msg_data)
                    elif msg_type == b'stdout':
                        if self.output_queue is None:
                            continue
                        await self.output_queue.put(
                            ResultRecord(
                                'stdout',
                                decoders[0].decode(msg_data),
                            ))
                    elif msg_type == b'stderr':
                        if self.output_queue is None:
                            continue
                        await self.output_queue.put(
                            ResultRecord(
                                'stderr',
                                decoders[1].decode(msg_data),
                            ))
                    else:
                        # Normal outputs should go to the current
                        # output queue.
                        if self.output_queue is None:
                            continue
                        await self.output_queue.put(
                            ResultRecord(
                                msg_type.decode('ascii'),
                                msg_data.decode('utf8'),
                            ))
                except asyncio.QueueFull:
                    pass
                if msg_type == b'build-finished':
                    # finalize incremental decoder
                    decoders[0].decode(b'', True)
                    decoders[1].decode(b'', True)
                elif msg_type == b'finished':
                    # finalize incremental decoder
                    decoders[0].decode(b'', True)
                    decoders[1].decode(b'', True)
                    self.finished_at = time.monotonic()
            except (asyncio.CancelledError, aiozmq.ZmqStreamClosed, GeneratorExit):
                break
            except Exception:
                log.exception('unexpected error')
                break
