using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Client.Framing;

namespace AddUp.RabbitMQ.Fakes
{
    internal sealed class FakeModel : IModel
    {
        private readonly ConcurrentDictionary<string, IBasicConsumer> consumers = new ConcurrentDictionary<string, IBasicConsumer>();
        private readonly RabbitServer server;
        private long lastDeliveryTag;

        public FakeModel(RabbitServer rabbitServer) => server = rabbitServer;

#pragma warning disable 67
        public event EventHandler<BasicAckEventArgs> BasicAcks;
        public event EventHandler<BasicNackEventArgs> BasicNacks;
        public event EventHandler<EventArgs> BasicRecoverOk;
        public event EventHandler<BasicReturnEventArgs> BasicReturn;
        public event EventHandler<CallbackExceptionEventArgs> CallbackException;
        public event EventHandler<FlowControlEventArgs> FlowControl;
        public event EventHandler<ShutdownEventArgs> ModelShutdown;
#pragma warning restore 67

        public readonly ConcurrentDictionary<ulong, RabbitMessage> WorkingMessages = new ConcurrentDictionary<ulong, RabbitMessage>();

        public int ChannelNumber { get; }
        public bool ApplyPrefetchToAllChannels { get; private set; }
        public ushort PrefetchCount { get; private set; }
        public uint PrefetchSize { get; private set; }
        public bool IsChannelFlowActive { get; private set; }
        public IBasicConsumer DefaultConsumer { get; set; }
        public ulong NextPublishSeqNo { get; set; }
        public TimeSpan ContinuationTimeout { get; set; }

        public ShutdownEventArgs CloseReason { get; private set; }
        public bool IsOpen => CloseReason == null;
        public bool IsClosed => !IsOpen;

        public void Dispose()
        {
            if (IsOpen) Abort();
        }

        public IEnumerable<RabbitMessage> GetMessagesPublishedToExchange(string exchange)
        {
            _ = server.Exchanges.TryGetValue(exchange, out var exchangeInstance);
            return exchangeInstance == null ?
                new List<RabbitMessage>() :
                (IEnumerable<RabbitMessage>)exchangeInstance.Messages;
        }

        public IEnumerable<RabbitMessage> GetMessagesOnQueue(string queueName)
        {
            _ = server.Queues.TryGetValue(queueName, out var queueInstance);
            return queueInstance == null ?
                new List<RabbitMessage>() :
                (IEnumerable<RabbitMessage>)queueInstance.Messages;
        }

        public IBasicProperties CreateBasicProperties() => new BasicProperties();
        public void ChannelFlow(bool active) => IsChannelFlowActive = active;
        public IBasicPublishBatch CreateBasicPublishBatch() => new FakeBasicPublishBatch(this);

        public void ExchangeDeclarePassive(string exchange)
        {
            if (!server.Exchanges.ContainsKey(exchange))
            {
                var shutdownArgs = new ShutdownEventArgs(initiator: ShutdownInitiator.Peer,
                    replyText: $"NOT_FOUND - no exchange '{exchange}' in vhost '/'",
                    replyCode: 404,
                    classId: 40,
                    methodId: 10);
                throw new OperationInterruptedException(shutdownArgs);
            }
        }

        public void ExchangeDeclare(string exchange, string type) => ExchangeDeclare(exchange, type, false, false, null);
        public void ExchangeDeclare(string exchange, string type, bool durable) => ExchangeDeclare(exchange, type, durable, false, null);
        public void ExchangeDeclareNoWait(string exchange, string type, bool durable, bool autoDelete, IDictionary<string, object> arguments) =>
            ExchangeDeclare(exchange, type, durable, false, arguments);
        public void ExchangeDeclare(string exchange, string type, bool durable, bool autoDelete, IDictionary<string, object> arguments)
        {
            var exchangeInstance = new RabbitExchange(type)
            {
                Name = exchange,
                IsDurable = durable,
                AutoDelete = autoDelete,
                Arguments = arguments
            };

            RabbitExchange updateFunction(string name, RabbitExchange existing) => existing;
            _ = server.Exchanges.AddOrUpdate(exchange, exchangeInstance, updateFunction);
        }

        public void ExchangeDelete(string exchange) => ExchangeDelete(exchange, false);
        public void ExchangeDeleteNoWait(string exchange, bool ifUnused) => ExchangeDelete(exchange, false);
        public void ExchangeDelete(string exchange, bool ifUnused) => server.Exchanges.TryRemove(exchange, out _);

        public void QueueBindNoWait(string queue, string exchange, string routingKey, IDictionary<string, object> arguments) => throw new NotImplementedException();
        public void QueueBind(string queue, string exchange, string routingKey) => ExchangeBind(queue, exchange, routingKey);
        public void QueueBind(string queue, string exchange, string routingKey, IDictionary<string, object> arguments) => ExchangeBind(queue, exchange, routingKey, arguments);

        public void ExchangeBindNoWait(string destination, string source, string routingKey, IDictionary<string, object> arguments) => throw new NotImplementedException();
        public void ExchangeBind(string destination, string source, string routingKey) => ExchangeBind(destination, source, routingKey, null);
        public void ExchangeBind(string destination, string source, string routingKey, IDictionary<string, object> arguments)
        {
            _ = server.Exchanges.TryGetValue(source, out var exchange);
            _ = server.Queues.TryGetValue(destination, out var queue);

            var binding = new RabbitExchangeQueueBinding { Exchange = exchange, Queue = queue, RoutingKey = routingKey };
            if (exchange != null)
                _ = exchange.Bindings.AddOrUpdate(binding.Key, binding, (k, v) => binding);
            if (queue != null)
                _ = queue.Bindings.AddOrUpdate(binding.Key, binding, (k, v) => binding);
        }

        public void QueueUnbind(string queue, string exchange, string routingKey, IDictionary<string, object> arguments) => ExchangeUnbind(queue, exchange, routingKey);

        public void ExchangeUnbindNoWait(string destination, string source, string routingKey, IDictionary<string, object> arguments) => throw new NotImplementedException();
        public void ExchangeUnbind(string destination, string source, string routingKey) => ExchangeUnbind(destination, source, routingKey, null);
        public void ExchangeUnbind(string destination, string source, string routingKey, IDictionary<string, object> arguments)
        {
            _ = server.Exchanges.TryGetValue(source, out var exchange);
            _ = server.Queues.TryGetValue(destination, out var queue);

            var binding = new RabbitExchangeQueueBinding { Exchange = exchange, Queue = queue, RoutingKey = routingKey };
            if (exchange != null)
                _ = exchange.Bindings.TryRemove(binding.Key, out _);
            if (queue != null)
                _ = queue.Bindings.TryRemove(binding.Key, out _);
        }

        public QueueDeclareOk QueueDeclare() => QueueDeclare(Guid.NewGuid().ToString(), false, false, false, null);
        public QueueDeclareOk QueueDeclarePassive(string queue)
        {
            if (server.Queues.TryGetValue(queue, out var rabbitQueue))
            {
                return new QueueDeclareOk(queue, (uint)unchecked(rabbitQueue.Messages.Count), (uint)unchecked(rabbitQueue.ConsumerCount));
            }
            var shutdownArgs = new ShutdownEventArgs(initiator: ShutdownInitiator.Peer,
                    replyText: $"NOT_FOUND - no queue '{queue}' in vhost '/'",
                    replyCode: 404,
                    classId: 50,
                    methodId: 10);
            throw new OperationInterruptedException(shutdownArgs);
        }

        public void QueueDeclareNoWait(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object> arguments) =>
            QueueDeclare(queue, durable, exclusive, autoDelete, arguments);
        public QueueDeclareOk QueueDeclare(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object> arguments)
        {
            // This handles 'default' queues creations with constructs such as:
            // var queueName = Channel.QueueDeclare(); // temporary anonymous queue
            var q = string.IsNullOrEmpty(queue) ? Guid.NewGuid().ToString() : queue;

            var queueInstance = new RabbitQueue
            {
                Name = q,
                IsDurable = durable,
                IsExclusive = exclusive,
                IsAutoDelete = autoDelete,
                Arguments = arguments
            };

            RabbitQueue updateFunction(string name, RabbitQueue existing) => existing;
            _ = server.Queues.AddOrUpdate(q, queueInstance, updateFunction);

            return new QueueDeclareOk(q, 0, 0);
        }

        public uint MessageCount(string queue) => throw new NotImplementedException();
        public uint ConsumerCount(string queue) => throw new NotImplementedException();

        public uint QueuePurge(string queue)
        {
            _ = server.Queues.TryGetValue(queue, out var instance);
            if (instance == null)
                return 0u;

            while (!instance.Messages.IsEmpty)
                _ = instance.Messages.TryDequeue(out _);

            return 1u;
        }

        public uint QueueDelete(string queue) => QueueDelete(queue, ifUnused: false, ifEmpty: false);
        public void QueueDeleteNoWait(string queue, bool ifUnused, bool ifEmpty) => QueueDelete(queue, false, false);
        public uint QueueDelete(string queue, bool ifUnused, bool ifEmpty)
        {
            _ = server.Queues.TryRemove(queue, out var instance);
            return instance != null ? 1u : 0u;
        }

        public void ConfirmSelect()
        {
            if (NextPublishSeqNo == 0ul)
                NextPublishSeqNo = 1ul;
        }

        public void WaitForConfirmsOrDie() => WaitForConfirmsOrDie(TimeSpan.Zero);
        public void WaitForConfirmsOrDie(TimeSpan timeout) => _ = WaitForConfirms(timeout);

        public bool WaitForConfirms() => WaitForConfirms(TimeSpan.Zero);
        public bool WaitForConfirms(TimeSpan timeout) => WaitForConfirms(timeout, out _);
        public bool WaitForConfirms(TimeSpan timeout, out bool timedOut)
        {
            if (NextPublishSeqNo == 0ul)
                throw new InvalidOperationException("Confirms not selected");

            timedOut = false;
            return true;
        }

        public string BasicConsume(string queue, bool autoAck, IBasicConsumer consumer) => BasicConsume(queue, autoAck, Guid.NewGuid().ToString(), true, false, null, consumer);
        public string BasicConsume(string queue, bool autoAck, string consumerTag, IBasicConsumer consumer) => BasicConsume(queue, autoAck, consumerTag, true, false, null, consumer);
        public string BasicConsume(string queue, bool autoAck, string consumerTag, IDictionary<string, object> arguments, IBasicConsumer consumer) => BasicConsume(queue, autoAck, consumerTag, true, false, arguments, consumer);
        public string BasicConsume(string queue, bool autoAck, string consumerTag, bool noLocal, bool exclusive, IDictionary<string, object> arguments, IBasicConsumer consumer)
        {
            _ = server.Queues.TryGetValue(queue, out var queueInstance);
            if (queueInstance != null)
            {
                IBasicConsumer updateFunction(string s, IBasicConsumer basicConsumer) => basicConsumer;
                _ = consumers.AddOrUpdate(consumerTag, consumer, updateFunction);

                NotifyConsumerOfExistingMessages(consumerTag, consumer, queueInstance);
                NotifyConsumerWhenMessagesAreReceived(consumerTag, consumer, queueInstance);
            }

            return consumerTag;
        }

        private void NotifyConsumerWhenMessagesAreReceived(string consumerTag, IBasicConsumer consumer, RabbitQueue queueInstance) =>
            queueInstance.MessagePublished += (sender, message) => NotifyConsumerOfMessage(consumerTag, consumer, message);

        private void NotifyConsumerOfExistingMessages(string consumerTag, IBasicConsumer consumer, RabbitQueue queueInstance)
        {
            foreach (var message in queueInstance.Messages)
                NotifyConsumerOfMessage(consumerTag, consumer, message);
        }

        private void NotifyConsumerOfMessage(string consumerTag, IBasicConsumer consumer, RabbitMessage message)
        {
            _ = Interlocked.Increment(ref lastDeliveryTag);

            var deliveryTag = Convert.ToUInt64(lastDeliveryTag);
            const bool redelivered = false;
            var exchange = message.Exchange;
            var routingKey = message.RoutingKey;
            var basicProperties = message.BasicProperties ?? CreateBasicProperties();
            var body = message.Body;

            RabbitMessage updateFunction(ulong key, RabbitMessage existingMessage) => existingMessage;
            _ = WorkingMessages.AddOrUpdate(deliveryTag, message, updateFunction);

            // In async mode, IBasicConsumer may 'hide' an IAsyncBasicConsumer...
            // See https://github.com/StephenCleary/AsyncEx/blob/e637035c775f99b50c458d4a90e330563ecfd07b/src/Nito.AsyncEx.Tasks/Synchronous/TaskExtensions.cs#L50
            // For why .GetAwaiter().GetResult()
            if (consumer is IAsyncBasicConsumer asyncBasicConsumer)
                asyncBasicConsumer
                    .HandleBasicDeliver(consumerTag, deliveryTag, redelivered, exchange, routingKey, basicProperties, body)
                    .GetAwaiter()
                    .GetResult();
            else consumer
                    .HandleBasicDeliver(consumerTag, deliveryTag, redelivered, exchange, routingKey, basicProperties, body);
        }

        public void BasicCancel(string consumerTag)
        {
            _ = consumers.TryRemove(consumerTag, out var consumer);
            if (consumer == null) return;

            // In async mode, IBasicConsumer may 'hide' an IAsyncBasicConsumer...
            // See https://github.com/StephenCleary/AsyncEx/blob/e637035c775f99b50c458d4a90e330563ecfd07b/src/Nito.AsyncEx.Tasks/Synchronous/TaskExtensions.cs#L50
            // For why .GetAwaiter().GetResult()
            if (consumer is IAsyncBasicConsumer asyncBasicConsumer)
                asyncBasicConsumer
                    .HandleBasicCancelOk(consumerTag)
                    .GetAwaiter()
                    .GetResult();
            else consumer
                    .HandleBasicCancelOk(consumerTag);
        }

        public BasicGetResult BasicGet(string queue, bool autoAck)
        {
            _ = server.Queues.TryGetValue(queue, out var queueInstance);
            if (queueInstance == null) return null;

            _ = autoAck ?
                queueInstance.Messages.TryDequeue(out var message) :
                queueInstance.Messages.TryPeek(out message);

            if (message == null) return null;

            _ = Interlocked.Increment(ref lastDeliveryTag);
            var deliveryTag = Convert.ToUInt64(lastDeliveryTag);
            const bool redelivered = false;
            var exchange = message.Exchange;
            var routingKey = message.RoutingKey;
            var messageCount = Convert.ToUInt32(queueInstance.Messages.Count);
            var basicProperties = message.BasicProperties ?? CreateBasicProperties();
            var body = message.Body;

            if (autoAck)
                _ = WorkingMessages.TryRemove(deliveryTag, out _);
            else
            {
                RabbitMessage updateFunction(ulong key, RabbitMessage existingMessage) => existingMessage;
                _ = WorkingMessages.AddOrUpdate(deliveryTag, message, updateFunction);
            }

            return new BasicGetResult(deliveryTag, redelivered, exchange, routingKey, messageCount, basicProperties, body);
        }

        public void BasicQos(uint prefetchSize, ushort prefetchCount, bool global)
        {
            PrefetchSize = prefetchSize;
            PrefetchCount = prefetchCount;
            ApplyPrefetchToAllChannels = global;
        }

        public void BasicPublish(PublicationAddress addr, IBasicProperties basicProperties, byte[] body) => BasicPublish(addr.ExchangeName, addr.RoutingKey, true, true, basicProperties, body);
        public void BasicPublish(string exchange, string routingKey, IBasicProperties basicProperties, byte[] body) => BasicPublish(exchange, routingKey, true, true, basicProperties, body);
        public void BasicPublish(string exchange, string routingKey, bool mandatory, IBasicProperties basicProperties, byte[] body) => BasicPublish(exchange, routingKey, mandatory, true, basicProperties, body);
        public void BasicPublish(string exchange, string routingKey, bool mandatory, bool immediate, IBasicProperties basicProperties, byte[] body)
        {
            var parameters = new RabbitMessage
            {
                Exchange = exchange,
                RoutingKey = routingKey,
                Mandatory = mandatory,
                Immediate = immediate,
                BasicProperties = basicProperties,
                Body = body
            };

            RabbitExchange addExchange(string s)
            {
                var newExchange = new RabbitExchange(ExchangeType.Direct)
                {
                    Name = exchange,
                    Arguments = null,
                    AutoDelete = false,
                    IsDurable = false
                };

                newExchange.PublishMessage(parameters);
                return newExchange;
            }

            RabbitExchange updateExchange(string s, RabbitExchange existingExchange)
            {
                existingExchange.PublishMessage(parameters);
                return existingExchange;
            }

            _ = server.Exchanges.AddOrUpdate(exchange, addExchange, updateExchange);

            if (NextPublishSeqNo != 0ul)
                NextPublishSeqNo++;
        }

        public void BasicAck(ulong deliveryTag, bool multiple)
        {
            _ = WorkingMessages.TryRemove(deliveryTag, out var message);
            if (message != null)
            {
                _ = server.Queues.TryGetValue(message.Queue, out var queue);
                if (queue != null)
                    _ = queue.Messages.TryDequeue(out _);
            }
        }

        public void BasicReject(ulong deliveryTag, bool requeue) => BasicNack(deliveryTag, false, requeue);
        public void BasicNack(ulong deliveryTag, bool multiple, bool requeue)
        {
            if (requeue) return;

            foreach (var queue in WorkingMessages.Select(m => m.Value.Queue))
            {
                _ = server.Queues.TryGetValue(queue, out var queueInstance);
                if (queueInstance != null)
                    queueInstance.ClearMessages();
            }

            _ = WorkingMessages.TryRemove(deliveryTag, out var message);
            if (message == null) return;

            foreach (var workingMessage in WorkingMessages)
            {
                _ = server.Queues.TryGetValue(workingMessage.Value.Queue, out var queueInstance);
                queueInstance?.PublishMessage(workingMessage.Value);
            }
        }

        public void BasicRecoverAsync(bool requeue) => BasicRecover(requeue);
        public void BasicRecover(bool requeue)
        {
            if (requeue)
            {
                foreach (var message in WorkingMessages)
                {
                    _ = server.Queues.TryGetValue(message.Value.Queue, out var queueInstance);
                    if (queueInstance != null)
                        queueInstance.PublishMessage(message.Value);
                }
            }

            WorkingMessages.Clear();
        }

        public void TxSelect() => throw new NotImplementedException();
        public void TxCommit() => throw new NotImplementedException();
        public void TxRollback() => throw new NotImplementedException();

        // Close and Abort (implementation inspired by RabbitMQ.Client)

        public void Abort() => Abort(200, "Goodbye");
        public void Abort(ushort replyCode, string replyText) => Close(replyCode, replyText, abort: true);

        public void Close() => Close(200, "Goodbye");
        public void Close(ushort replyCode, string replyText) => Close(replyCode, replyText, abort: false);

        private void Close(ushort replyCode, string replyText, bool abort) =>
            Close(new ShutdownEventArgs(ShutdownInitiator.Application, replyCode, replyText), abort);

        private void Close(ShutdownEventArgs reason, bool abort)
        {
            try
            {
                CloseReason = reason;
                ModelShutdown?.Invoke(this, reason);
            }
            catch
            {
                if (!abort) throw;
            }
        }
    }
}