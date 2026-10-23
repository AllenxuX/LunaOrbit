const labels: Record<string, string> = {
  'retention.ms':'消息保留时间', 'retention.bytes':'分区容量上限', 'cleanup.policy':'清理策略',
  'min.insync.replicas':'最小同步副本数', 'max.message.bytes':'单条消息上限', 'segment.bytes':'日志段大小',
  'segment.ms':'日志段滚动时间', 'delete.retention.ms':'删除标记保留时间', 'compression.type':'压缩方式',
  'unclean.leader.election.enable':'非同步副本选主', 'message.timestamp.type':'消息时间戳',
  'log.retention.ms':'消息保留时间', 'log.segment.bytes':'日志段大小', 'num.partitions':'默认分区数',
  'default.replication.factor':'默认副本数', 'auto.create.topics.enable':'自动创建 Topic',
  'delete.topic.enable':'允许删除 Topic', 'log.cleanup.policy':'默认清理策略',
}
const sources: Record<string,string> = {DYNAMIC_TOPIC_CONFIG:'Topic 覆盖',DYNAMIC_BROKER_CONFIG:'Broker 覆盖',DYNAMIC_DEFAULT_BROKER_CONFIG:'集群默认覆盖',STATIC_BROKER_CONFIG:'Broker 启动配置',DEFAULT_CONFIG:'Kafka 默认',UNKNOWN_CONFIG:'Kafka 未返回',NO_CLUSTER_DEFAULT_OVERRIDE:'未设置集群覆盖',DYNAMIC_OVERRIDE:'设置覆盖值',INHERITED:'恢复继承'}
const descriptions: Record<string,string> = {
  'min.insync.replicas':'生产者使用 acks=all 时，同步副本数不能低于此值，否则写入失败。例如副本数为 3、此值为 2，ISR 少于 2 时拒绝写入。',
  'retention.ms':'使用删除策略时，按此时间清理过期日志段。单位为毫秒，-1 表示不按时间限制保留。',
  'retention.bytes':'使用删除策略时，每个分区允许保留的日志容量。单位为字节，-1 表示不按容量限制保留。',
  'cleanup.policy':'日志清理方式：delete 删除过期日志段；compact 按消息键压缩，保留较新的值。两种策略可以组合使用。',
  'max.message.bytes':'Broker 接受的消息记录批次大小上限，单位为字节；使用压缩时按压缩后的批次大小判断。',
  'segment.bytes':'单个日志段的大小上限，达到上限后滚动创建新日志段。单位为字节。',
  'segment.ms':'日志段达到此时间后滚动创建新日志段。单位为毫秒。',
  'segment.jitter.ms':'日志段滚动时间的随机扰动范围，避免多个分区同时滚动。单位为毫秒。',
  'segment.index.bytes':'单个日志段索引文件的大小上限，单位为字节。',
  'index.interval.bytes':'日志中建立偏移量索引的字节间隔。间隔较小会增加索引项。',
  'delete.retention.ms':'压缩日志中删除标记的保留时间，为消费者处理删除记录预留时间。单位为毫秒。',
  'file.delete.delay.ms':'日志文件被标记删除后，实际删除前等待的时间。单位为毫秒。',
  'compression.type':'消息压缩方式。producer 表示沿用生产者选择的压缩方式；其他值指定 Broker 使用的压缩方式。',
  'unclean.leader.election.enable':'是否允许非同步副本成为 Leader。开启可提高部分故障下的可用性，但可能丢失消息。',
  'message.timestamp.type':'消息时间戳来源：CreateTime 使用生产者时间；LogAppendTime 使用 Broker 追加日志的时间。',
  'message.timestamp.before.max.ms':'消息时间戳允许早于 Broker 时间的最大差值。超过限制的消息会被拒绝，单位为毫秒。',
  'message.timestamp.after.max.ms':'消息时间戳允许晚于 Broker 时间的最大差值。超过限制的消息会被拒绝，单位为毫秒。',
  'message.timestamp.difference.max.ms':'消息时间戳与 Broker 时间允许的最大差值。单位为毫秒；适用规则由 Kafka 版本决定。',
  'message.format.version':'写入日志使用的消息格式版本，兼容性由 Broker 校验。',
  'message.downconversion.enable':'是否允许向旧版消费者转换消息格式。转换可能增加 Broker 的资源开销。',
  'min.cleanable.dirty.ratio':'日志压缩的脏数据比例阈值，用于决定日志是否满足压缩条件。',
  'min.compaction.lag.ms':'消息写入后至少保留多久才允许被日志压缩处理。单位为毫秒。',
  'max.compaction.lag.ms':'控制消息允许等待日志压缩的最长时间。单位为毫秒。',
  'flush.messages':'累计写入指定数量的消息后触发日志刷盘。',
  'flush.ms':'日志刷盘的时间间隔，单位为毫秒。',
  'preallocate':'是否在创建新日志段时预先分配文件空间。',
  'leader.replication.throttled.replicas':'需要限制复制发送速率的分区和副本列表，配合 Broker 的复制节流速率使用。',
  'follower.replication.throttled.replicas':'需要限制复制接收速率的分区和副本列表，配合 Broker 的复制节流速率使用。',
  'remote.storage.enable':'是否为此 Topic 启用分层存储，需要 Broker 已配置相应存储支持。',
  'local.retention.ms':'分层存储场景下，本地日志的保留时间。单位为毫秒，-2 表示继承 Topic 的保留时间。',
  'local.retention.bytes':'分层存储场景下，每个分区本地日志的容量限制。单位为字节，-2 表示继承 Topic 的容量限制。',
  'num.partitions':'创建 Topic 时未指定分区数所使用的默认值。',
  'default.replication.factor':'自动创建 Topic 时使用的默认副本数。',
  'auto.create.topics.enable':'是否允许客户端请求触发自动创建 Topic。',
  'delete.topic.enable':'是否允许删除 Topic。关闭后，删除请求不会按正常删除流程执行。',
  'log.retention.hours':'Broker 默认的日志保留时间，单位为小时。与其他时间单位配置并存时，生效值由 Kafka 的优先级规则决定。',
  'log.retention.minutes':'Broker 默认的日志保留时间，单位为分钟。与毫秒配置并存时，生效值由 Kafka 的优先级规则决定。',
  'log.retention.check.interval.ms':'检查日志是否满足删除条件的时间间隔，单位为毫秒。',
  'log.dirs':'存放 Kafka 日志数据的目录列表。',
  'log.dir':'存放 Kafka 日志数据的目录；配置了目录列表时按目录列表处理。',
  'log.cleaner.enable':'是否启用日志压缩清理器，用于处理采用 compact 策略的日志。',
  'log.cleaner.threads':'执行日志压缩清理的线程数。',
  'log.cleaner.io.max.bytes.per.second':'日志清理器的读写吞吐限制，单位为字节每秒。',
  'log.cleaner.dedupe.buffer.size':'日志压缩清理使用的去重缓冲区大小，单位为字节。',
  'log.cleaner.io.buffer.size':'日志清理器使用的读写缓冲区大小，单位为字节。',
  'num.network.threads':'处理网络请求的线程数。',
  'num.io.threads':'处理请求及磁盘读写的工作线程数。',
  'num.replica.fetchers':'从其他 Broker 拉取副本数据的线程数。',
  'num.recovery.threads.per.data.dir':'每个数据目录在日志恢复和关闭时使用的线程数。',
  'socket.send.buffer.bytes':'网络连接的发送缓冲区大小，单位为字节。',
  'socket.receive.buffer.bytes':'网络连接的接收缓冲区大小，单位为字节。',
  'socket.request.max.bytes':'单个网络请求允许的最大大小，单位为字节。',
  'queued.max.requests':'等待工作线程处理的请求数量上限。',
  'connections.max.idle.ms':'空闲连接被关闭前允许保持的时间，单位为毫秒。',
  'max.connections':'允许建立的连接数量上限。',
  'max.connections.per.ip':'每个客户端地址允许建立的连接数量上限。',
  'listeners':'Broker 监听的协议名称、地址和端口列表。',
  'advertised.listeners':'发布给客户端使用的连接地址和端口列表，客户端必须能够访问这些地址。',
  'listener.security.protocol.map':'监听器名称到连接安全协议的映射。',
  'inter.broker.listener.name':'Broker 之间通信所使用的监听器名称。',
  'security.inter.broker.protocol':'Broker 之间通信所使用的安全协议。',
  'sasl.enabled.mechanisms':'允许客户端使用的认证机制列表。',
  'sasl.mechanism.inter.broker.protocol':'Broker 之间通信所使用的认证机制。',
  'ssl.client.auth':'是否要求客户端提供证书进行认证。',
  'ssl.enabled.protocols':'允许使用的安全传输协议版本列表。',
  'ssl.endpoint.identification.algorithm':'安全连接校验服务端主机名所使用的算法。',
  'broker.id':'Broker 的节点编号。',
  'node.id':'节点编号，用于标识集群中的 Broker 或控制器。',
  'process.roles':'此节点承担的角色，可以是 Broker、控制器或二者。',
  'controller.listener.names':'控制器通信所使用的监听器名称列表。',
  'controller.quorum.voters':'控制器投票节点的编号及连接地址列表。',
  'offsets.topic.replication.factor':'消费位点内部 Topic 创建时使用的副本数。',
  'offsets.retention.minutes':'满足清理条件的消费组位点保留时间，单位为分钟。',
  'transaction.state.log.replication.factor':'事务状态内部 Topic 创建时使用的副本数。',
  'transaction.state.log.min.isr':'事务状态内部 Topic 所要求的最小同步副本数。',
  'group.initial.rebalance.delay.ms':'新消费组首次分配分区前等待其他成员加入的时间，单位为毫秒。',
  'group.min.session.timeout.ms':'允许消费者设置的最短会话超时时间，单位为毫秒。',
  'group.max.session.timeout.ms':'允许消费者设置的最长会话超时时间，单位为毫秒。',
  'replica.lag.time.max.ms':'跟随副本长时间未拉取或未追上 Leader 时，将其移出 ISR 的时间阈值，单位为毫秒。',
  'replica.fetch.max.bytes':'副本复制时，每个分区一次拉取的大小限制，单位为字节。',
  'replica.fetch.response.max.bytes':'副本复制时，一次拉取响应的总大小限制，单位为字节。',
  'leader.replication.throttled.rate':'Leader 复制发送的节流速率，单位为字节每秒，配合选定的节流副本使用。',
  'follower.replication.throttled.rate':'跟随副本复制接收的节流速率，单位为字节每秒，配合选定的节流副本使用。',
}
const descriptionAliases:Record<string,string> = {
  'log.message.timestamp.type':'message.timestamp.type', 'message.max.bytes':'max.message.bytes',
  'log.message.format.version':'message.format.version', 'log.message.downconversion.enable':'message.downconversion.enable',
  'log.index.size.max.bytes':'segment.index.bytes', 'log.roll.ms':'segment.ms', 'log.roll.jitter.ms':'segment.jitter.ms',
  'log.flush.interval.messages':'flush.messages', 'log.flush.interval.ms':'flush.ms',
  'log.cleaner.min.cleanable.ratio':'min.cleanable.dirty.ratio', 'log.cleaner.min.compaction.lag.ms':'min.compaction.lag.ms',
  'log.cleaner.max.compaction.lag.ms':'max.compaction.lag.ms',
}
export function configDescription(name:string):string {
  const listenerBase = name.startsWith('listener.name.') ? name.split('.').slice(3).join('.') : name
  return descriptions[name] ?? descriptions[descriptionAliases[name]??''] ?? descriptions[name.startsWith('log.')?name.slice(4):''] ?? descriptions[listenerBase] ?? '暂无中文说明。'
}
export const configLabel = (name:string) => labels[name] ?? name
const common = ['retention.ms','cleanup.policy','retention.bytes','min.insync.replicas','max.message.bytes','compression.type','log.retention.ms','log.cleanup.policy','num.partitions','default.replication.factor','auto.create.topics.enable','delete.topic.enable']
export const configIsCommon = (name:string) => common.includes(name)
export const configOrder = (name:string) => common.includes(name) ? common.indexOf(name) : 100
export const configSource = (source:string) => sources[source] ?? source
export function configValue(name:string,value:string|null|undefined):string {
  if (value == null) return '未知'
  if (value === '-1' && /(?:retention\.(?:ms|bytes)|log\.retention\.ms)$/.test(name)) return '不限制（-1）'
  if (value === 'true') return '开启'
  if (value === 'false') return '关闭'
  const number = Number(value)
  if (value.trim() && Number.isFinite(number) && number >= 0) {
    if (name.endsWith('.ms')) {
      for (const [unit,divisor] of [['天',86400000],['小时',3600000],['分钟',60000],['秒',1000]] as const) if(number>=divisor && number%divisor===0) return `${number/divisor} ${unit}`
      return `${value} 毫秒`
    }
    if (name.endsWith('.bytes')) {
      for (const [unit,divisor] of [['GiB',1073741824],['MiB',1048576],['KiB',1024]] as const) if(number>=divisor && number%divisor===0) return `${number/divisor} ${unit}`
      return `${value} B`
    }
  }
  return value
}
