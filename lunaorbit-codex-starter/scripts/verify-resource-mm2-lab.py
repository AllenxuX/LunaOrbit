"""Repeat the guarded MM2 marker regression with bounded Kafka CLI heaps."""
from importlib.machinery import SourceFileLoader
from pathlib import Path
import secrets
import time

guard = SourceFileLoader('resource_mm2_guard', str(Path(__file__).with_name('setup-resource-broker-lab.py'))).load_module()
guard.inspect()
lab = guard.lab
original = lab.kubectl


def bounded_cli(*args, **kwargs):
    if args and args[0] == 'exec' and '--' in args:
        split = args.index('--') + 1
        args = (*args[:split], 'env', 'KAFKA_HEAP_OPTS=-Xms32m -Xmx64m', *args[split:])
    return original(*args, **kwargs)


def cli(broker, tool, *args, data=None):
    return guard.scram.quiet_kubectl('exec', *(['-i'] if data is not None else []), 'deploy/'+broker, '--',
        'env', 'KAFKA_HEAP_OPTS=-Xms32m -Xmx64m', '/opt/kafka/bin/'+tool,
        '--bootstrap-server', 'localhost:9092', *args, data=data)


topic = 'lunaorbit-lab-events'
remote = 'source.' + topic
marker = 'lunaorbit-resources-marker-' + secrets.token_hex(12)
cli('kafka-source', 'kafka-topics.sh', '--create', '--if-not-exists', '--topic', topic, '--partitions', '1', '--replication-factor', '1')
cli('kafka-source', 'kafka-console-producer.sh', '--topic', topic, data=marker+'\n')
for attempt in range(8):
    time.sleep(3)
    def offset(when):
        value = cli('kafka-target', 'kafka-get-offsets.sh', '--topic', remote, '--time', when)
        rows = [line for line in value.splitlines() if line.startswith(remote+':0:')]
        return int(rows[0].rsplit(':',1)[1]) if rows else 0
    end, earliest = offset('-1'), offset('-2')
    start = max(earliest, end-100)
    if end <= start:
        continue
    # Old segments may already have expired. Offset zero is not always readable.
    output = cli('kafka-target', 'kafka-console-consumer.sh', '--topic', remote, '--partition', '0',
        '--offset', str(start), '--max-messages', str(end-start), '--timeout-ms', '10000')
    if marker in output.splitlines():
        print('MM2_REPLICATION_PASS: new unique marker copied from Kafka3.7.2 to Kafka3.9.2')
        break
else:
    raise AssertionError('New synthetic MM2 marker was not observed in bounded target records')
