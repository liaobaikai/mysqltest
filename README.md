
**启用semi-sync的正确方式**  

默认情况下的mysql复制都是异步复制，mysql通过参数来控制semi-sync开关。具体而言，主库上通过rpl_semi_sync_master_enabled参数控制，备库上通过rpl_semi_sync_slave_enabled参数控制，打开这两个参数后，mysql semi-sync的特性就打开了。注意对于备库而言，为了保证半同步立即生效，需要重启slave的IO线程。另外，还有一个比较重要的参数是rpl_semi_sync_master_timeout，这个参数用于控制master等待semi-slave ack报文的时间，单位是毫秒，默认是10000。master等待超时，则切换为普通的异步复制。


```
master:
set global rpl_semi_sync_master_enabled=1;
set global rpl_semi_sync_master_timeout=10000;

slave:
stop slave io_thread;
set global rpl_semi_sync_slave_enabled=1;
start slave io_thread;

*simulate-slave:
SET @rpl_semi_sync_replica = 1, @rpl_semi_sync_slave = 1;
```

另外需要注意的是，打开了上述两个参数rpl_semi_sync_master_enabled&rpl_semi_sync_slave_enabled只说明master-slave已经具备打开semi-sync的基本条件了，但复制是否依照半同步运行，还需要根据Rpl_semi_sync_master_status的状态值确定。如果slave较master有很大延迟(超过rpl_semi_sync_master_timeout)，那么复制切换为普通复制。

```
mysql> show global status like '%semi_sync%';
+--------------------------------------------+-------+
| Variable_name                              | Value |
+--------------------------------------------+-------+
| Rpl_semi_sync_master_clients               | 1     |
| Rpl_semi_sync_master_net_avg_wait_time     | 0     |
| Rpl_semi_sync_master_net_wait_time         | 0     |
| Rpl_semi_sync_master_net_waits             | 0     |
| Rpl_semi_sync_master_no_times              | 0     |
| Rpl_semi_sync_master_no_tx                 | 0     |
| Rpl_semi_sync_master_status                | ON    | <<<
| Rpl_semi_sync_master_timefunc_failures     | 0     |
| Rpl_semi_sync_master_tx_avg_wait_time      | 0     |
| Rpl_semi_sync_master_tx_wait_time          | 0     |
| Rpl_semi_sync_master_tx_waits              | 0     |
| Rpl_semi_sync_master_wait_pos_backtraverse | 0     |
| Rpl_semi_sync_master_wait_sessions         | 0     |
| Rpl_semi_sync_master_yes_tx                | 0     |
+--------------------------------------------+-------+
14 rows in set (0.01 sec)
```

> 主库和备库分别有rpl_semi_sync_master_trace_level和rpl_semi_sync_slave_trace_level参数来控制半同步复制打印日志。
将两个参数值设置为112(64+32+16)，记录详细日志信息（默认）、网络等待信息，以及进出的函数调用。


**semi-sync的实现**  
semi-sync说到底也是一种复制，只不过是在异步的复制基础上，添加了一些步骤来实现。因此semi-sync并没有改变复制的基本框架，我们的讨论也从master，slave两方面展开。 


**1. master(主库)**  
主库上面主要包含三个部分：  
(1) 负责与slave-io线程对接的binlog dump线程，将binlog发送到slave  
(2) 主库上写事务的工作线程  
(3) 收取semi-slave报文的ack_receiver线程。  

**binlog dump流程**  

主要执行逻辑在 `repl_semi_binlog_dump_start` 函数中。  
.\mysql-8.0.39\plugin\semisync\semisync_source_plugin.cc:: repl_semi_binlog_dump_start    
1. 判断slave是否是semi_slave，调用add_slave将semi-slave加入到ack_receiver线程的监听队列中。判断的逻辑是slave对应的会话上是否设置了参数 `@rpl_semi_sync_replica` 或 `@rpl_semi_sync_slave` 。

2. 根据slave的请求偏移和binlog文件，从指定位点读取binlog  
3. 根据文件和位点，读取binlog文件中的数据
.\mysql-8.0.39\sql\rpl_source.cc:: mysql_binlog_send  
.\mysql-8.0.39\sql\rpl_binlog_sender.cc:: Binlog_sender::run()    

4. 在函数 `repl_semi_before_send_event` 调用 `updateSyncHeader` 设置数据包头semi-sync标记  
.\mysql-8.0.39\plugin\semisync\semisync_source_plugin.cc:: repl_semi_before_send_event  
..\mysql-8.0.39\plugin\semisync\semisync_source.cc:: updateSyncHeader  
根据实时semi-sync运行状态来确定是否设置(这个状态由ack_receiver线程根据是否及时收到slave-ack报文设置)

5. 调用`my_net_write`发送binlog  
.\mysql-8.0.39\sql\rpl_binlog_sender.cc:: Binlog_sender::send_packet()

6. 调用`flush_net`确认网络包发送出去  


**接收slave-ack报文流程**  

这个流程的工作主要在ack_receiver线程中，这个线程的主要作用是监听semi-slave的ack包，确认master-slave链路是否工作在半同步状态，并根据实际运行状态将普通复制与半同步复制进行切换。打开主库rpl_semi_sync_master_enabled参数后，该线程启动，关闭参数后，该线程消亡。  
**流程如下：**  
.\mysql-8.0.39\plugin\semisync\semisync_source_ack_receiver.cc:: Ack_receiver::run()  
1. 遍历semi-slave数组  
2. 通过select函数监听每个slave是否有网络包过来  
3. 调用my_net_read读取包数据  
4. 调用reportReplyPacket处理semi-sync复制状态  
5. 若备库已经获取了最新的binlog位点，则唤醒等待的工作线程  
   .\mysql-8.0.39\plugin\semisync\semisync_source.cc
   调用reportReplyBinlog唤醒等待的线程，mysql_cond_broadcast(&m_cond)  


**2.slave(备库)**  

主要实现在(handle_slave_io)
1. 启动io-thread后，调用safe_connect建立与master的连接  
2. 调用request_dump函数处理请求binlog逻辑  
(1). 执行命令SET @rpl_semi_sync_slave= 1，设置一个局部变量，通过这个参数标记slave为semi-slave  
(2). 发送命令COM_BINLOG_DUMP请求日志  
循环从master端收取日志，处理日志  
{  
　　1. 调用read_event，从master端收取日志(如果没有网络包，会阻塞等待)  
　　2. 调用slaveReadSyncHeader，确定网络包头是否有semi-sync标记  
　　3. 调用queue_event将日志写入relay-log，在这个过程中会过滤自身server-id的日志  
　　4. 如果有semi-sync标记，调用slaveReply函数，发送ack报文  
      .\mysql-8.0.39\plugin\semisync\semisync_replica.cc:: ReplSemiSyncSlave::slaveReply -> my_net_write -> net_flush  
}  

---

semi_sync数据包说明：  
```
.\mysql-8.0.39\plugin\semisync\semisync.cc

ReplSemiSyncBase::kPacketMagicNum = 0xef;
ReplSemiSyncBase::kPacketFlagSync = 0x01;
..
ReplSemiSyncBase::kSyncHeader[2] = { ReplSemiSyncBase::kPacketMagicNum, 0};

kPacketMagicNum：是网络数据包的魔术字头，用于标识半同步通信协议的起始标记。  
kPacketFlagSync：是事务事件标志位，主库通过该标志标识需要从库ACK反馈的事务事件。  

工作流程‌  
主库通过repl_semi_before_send_event回调函数判断事务事件是否为最后一个event，若是则设置kPacketFlagSync=1，触发从库ACK反馈。  
从库通过repl_semi_slave_queue_event回调检测该标志，决定是否发送ACK。  

协议设计‌  
两者均为ReplSemiSyncBase类的静态常量，定义在二进制通信协议头部。  
与MySQL的变长整数编码协议（如int<lenenc>）协同工作，实现高效网络传输。  

超时机制关联‌  
若从库未在rpl_semi_sync_master_timeout（默认1秒）内返回ACK，主库会退化为异步复制，此时这些标志位将不再生效。

如数据包：
十进制字节：0, 239, 0, 0, 0, 0, 0, 4, 1, 0, 0, 0, 40, 0, 0, 0, 0, 0, 0, 0, 32, 0, 157, 0, 0, 0, 0, 0, 0, 0, 98, 105, 110, 108, 111, 103, 46, 48, 48, 48, 48, 48, 50
十六进制对应：00, ef, 00, 00, 00, 00, 00, 04, 01, 00, 00, 00, 28, 00, 00, 00, 00, 00, 00, 00, 20, 00, 9d, 00, 00, 00, 00, 00, 00, 00, 62, 69, 6e, 6c, 6f, 67, 2e, 30, 30, 30, 30, 30, 32

第二字节：0xef 表示半同步复制
第三字节：0x00 表示不需要从备库返回ack

/* Replication event packet header:
  *  . slave semi-sync off: 1 byte - (0)
  *  . slave semi-sync on:  3 byte - (0, 0xef, 0/1}
  */

```

**对于数据包的解释，还在查阅其他资料。**  


semi_sync ACK数据包说明：  
```
.\mysql-8.0.39\plugin\semisync\semisync.h

/* The layout of a semisync slave reply packet:
   1 byte for the magic num
   8 bytes for the binlog position
   n bytes for the binlog filename, terminated with a '\0'
*/

```


**模拟测试情况：**
```
语言：Rust
依赖库：mysql-async
测试进展：程序启动以半同步模式连接到主库，主库返回的数据包mysql-common解析不了，测试中断（已反馈给mysql-async官方）
下一步：继续查找资料
说明：其他语言，比如C的依赖库可能兼容，未测试过

MYSQL官方开发文档：
https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_replication_binlog_event.html#sect_protocol_replication_event_format_desc
```


---


**问题：**
1. 确认在slave启动后，同步的第一批binlog(从故障点到当前的积压日志) ，此时的模式是半同步还是异步获取日志？
2. 当Master Rpl_semi_sync_master_status=OFF 时， 是否连接上的所有slave都是异步复制。
3. 同1, 当Master Rpl_semi_sync_master_status=ON 时， 连接上来的是异步复制，需要追上积压日志后会自动转为半同步复制？
4. 首次连接按位点获取日志时，这个位点从哪里取？（待定）
5. Master根据什么信号判断连接过来的slave是采用半同步模式还是异步模式？


**Master Status:**
```
mysql> show global status like '%semi_sync%';
+--------------------------------------------+-------+
| Variable_name                              | Value |
+--------------------------------------------+-------+
| Rpl_semi_sync_master_clients               | 1     |
| Rpl_semi_sync_master_net_avg_wait_time     | 0     |
| Rpl_semi_sync_master_net_wait_time         | 0     |
| Rpl_semi_sync_master_net_waits             | 0     |
| Rpl_semi_sync_master_no_times              | 0     |
| Rpl_semi_sync_master_no_tx                 | 0     |
| Rpl_semi_sync_master_status                | ON    | <<<
| Rpl_semi_sync_master_timefunc_failures     | 0     |
| Rpl_semi_sync_master_tx_avg_wait_time      | 0     |
| Rpl_semi_sync_master_tx_wait_time          | 0     |
| Rpl_semi_sync_master_tx_waits              | 0     |
| Rpl_semi_sync_master_wait_pos_backtraverse | 0     |
| Rpl_semi_sync_master_wait_sessions         | 0     |
| Rpl_semi_sync_master_yes_tx                | 0     |
+--------------------------------------------+-------+
14 rows in set (0.01 sec)

mysql> show variables like '%semi_sync%';
+-------------------------------------------+------------+
| Variable_name                             | Value      |
+-------------------------------------------+------------+
| rpl_semi_sync_master_enabled              | ON         | <<< 前提条件
| rpl_semi_sync_master_timeout              | 10000      |
| rpl_semi_sync_master_trace_level          | 32         |
| rpl_semi_sync_master_wait_for_slave_count | 1          |
| rpl_semi_sync_master_wait_no_slave        | ON         |
| rpl_semi_sync_master_wait_point           | AFTER_SYNC |
+-------------------------------------------+------------+
6 rows in set (0.01 sec)
```


**2. 当Master Rpl_semi_sync_master_status=OFF 时， 是否连接上的所有slave都是异步复制。**
YES

```
.\mysql-8.0.39\plugin\semisync\semisync_source.cc

int ReplSemiSyncMaster::writeTranxInBinlog(const char *log_file_name,
                                           my_off_t log_file_pos) {

...

if (is_on()) {
    assert(active_tranxs_ != nullptr);
    if (active_tranxs_->insert_tranx_node(log_file_name, log_file_pos)) {
      /*
        if insert tranx_node failed, print a warning message
        and turn off semi-sync
      */
      LogErr(WARNING_LEVEL, ER_SEMISYNC_FAILED_TO_INSERT_TRX_NODE,
             log_file_name, (ulong)log_file_pos);
      switch_off();
    }
  }
```


**3. 同1, 当Master Rpl_semi_sync_master_status=ON 时， 连接上来的是异步复制，需要追上积压日志后会自动转为半同步复制？**

**如果当前发送事件的位置大于或等于“最大”提交事务binlog位置，则Slave现在已经赶上了，我们可以在这里切换半同步。
如果commit_file_name_inited_表示最近没有事务，我们可以立即启用半同步。**
```
.\mysql-8.0.39\plugin\semisync\semisync_source.cc

int ReplSemiSyncMaster::try_switch_on(const char *log_file_name,
                                      my_off_t log_file_pos) {
  const char *kWho = "ReplSemiSyncMaster::try_switch_on";
  bool semi_sync_on = false;

  function_enter(kWho);

  /* If the current sending event's position is larger than or equal to the
   * 'largest' commit transaction binlog position, the slave is already
   * catching up now and we can switch semi-sync on here.
   * If commit_file_name_inited_ indicates there are no recent transactions,
   * we can enable semi-sync immediately.
   */
  if (commit_file_name_inited_) {
    int cmp = ActiveTranx::compare(log_file_name, log_file_pos,
                                   commit_file_name_, commit_file_pos_);
    semi_sync_on = (cmp >= 0);
  } else {
    semi_sync_on = true;
  }

  if (semi_sync_on) {
    /* Switch semi-sync replication on. */
    state_ = true;

    LogErr(INFORMATION_LEVEL, ER_SEMISYNC_RPL_SWITCHED_ON, log_file_name,
           (unsigned long)log_file_pos);
  }

  return function_exit(kWho, 0);
}
```

当Rpl_semi_sync_master_status=ON 时，更新数据包报头中的同步位，以向Slave指示Master是否会等待事件的回复。
如果半同步Rpl_semi_sync_master_status=OFF，我们检测到Slave赶上，我们就会打开半同步。

```
.\mysql-8.0.39\plugin\semisync\semisync_source.cc

int ReplSemiSyncMaster::updateSyncHeader(unsigned char *packet,
                                         const char *log_file_name,
                                         my_off_t log_file_pos,
                                         uint32 server_id) {

  ...

  if (is_on()) {
    /* semi-sync is ON */
    /* sync= false; No sync unless a transaction is involved. */

    if (reply_file_name_inited_) {
      cmp = ActiveTranx::compare(log_file_name, log_file_pos, reply_file_name_,
                                 reply_file_pos_);
      if (cmp <= 0) {
        /* If we have already got the reply for the event, then we do
         * not need to sync the transaction again.
         */
        goto l_end;
      }
    }

    if (wait_file_name_inited_) {
      cmp = ActiveTranx::compare(log_file_name, log_file_pos, wait_file_name_,
                                 wait_file_pos_);
    } else {
      cmp = 1;
    }

    /* If we are already waiting for some transaction replies which
     * are later in binlog, do not wait for this one event.
     */
    if (cmp >= 0) {
      /*
       * We only wait if the event is a transaction's ending event.
       */
      assert(active_tranxs_ != nullptr);
      sync = active_tranxs_->is_tranx_end_pos(log_file_name, log_file_pos);
    }
  } else {
    if (commit_file_name_inited_) {
      int cmp = ActiveTranx::compare(log_file_name, log_file_pos,
                                     commit_file_name_, commit_file_pos_);
      sync = (cmp >= 0);
    } else {
      sync = true;
    }
  }

  ...

l_end:
  unlock();

  /* We do not need to clear sync flag because we set it to 0 when we
   * reserve the packet header.
   */
  if (sync) {
    (packet)[2] = kPacketFlagSync;
  }

  return function_exit(kWho, 0);
}
```




读取slave返回的响应，根据响应头kPacketFlagSync是否已经有ack响应
```
.\mysql-8.0.39\plugin\semisync\semisync_source.cc

int ReplSemiSyncMaster::readSlaveReply(NET *net, const char *event_buf) {
  const char *kWho = "ReplSemiSyncMaster::readSlaveReply";
  int result = -1;

  function_enter(kWho);

  assert((unsigned char)event_buf[1] == kPacketMagicNum);
  if ((unsigned char)event_buf[2] != kPacketFlagSync) {
    /* current event does not require reply */
    result = 0;
    goto l_end;
  }

  /* We flush to make sure that the current event is sent to the network,
   * instead of being buffered in the TCP/IP stack.
   */
  if (net_flush(net)) {
    LogErr(ERROR_LEVEL, ER_SEMISYNC_SOURCE_FAILED_ON_NET_FLUSH);
    goto l_end;
  }

  net_clear(net, false);
  net->pkt_nr++;
  result = 0;
  rpl_semi_sync_source_net_wait_num++;

l_end:
  return function_exit(kWho, result);
}
```



**5. Master根据什么信号判断连接过来的slave是采用半同步模式还是异步模式？**

MySQL根据slave提供rpl_semi_sync_replica来判断是否为半同步模式的slave
```
.\mysql-8.0.39\plugin\semisync\semisync_source.cc

bool ReplSemiSyncMaster::is_semi_sync_slave() {
  int null_value;
  long long val = 0;
  get_user_var_int("rpl_semi_sync_replica", &val, &null_value);
  return val;
}
```