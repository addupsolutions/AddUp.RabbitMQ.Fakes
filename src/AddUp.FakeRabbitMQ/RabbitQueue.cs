using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace AddUp.RabbitMQ.Fakes
{
    internal sealed class RabbitQueue
    {
        public RabbitQueue()
        {
            Messages = new ConcurrentQueue<RabbitMessage>();
            Bindings = new ConcurrentDictionary<string, RabbitExchangeQueueBinding>();
            Arguments = new Dictionary<string, object>();
        }

        private readonly HashSet<EventHandler<RabbitMessage>> messagePublished = new HashSet<EventHandler<RabbitMessage>>();
        public event EventHandler<RabbitMessage> MessagePublished
        {
            add { messagePublished.Add(value); }
            remove { messagePublished.Remove(value); }
        }

        public ConcurrentQueue<RabbitMessage> Messages { get; }
        public ConcurrentDictionary<string, RabbitExchangeQueueBinding> Bindings { get; }
        public IDictionary<string, object> Arguments { get; set; }
        public string Name { get; set; }
        public bool IsDurable { get; set; }
        public bool IsExclusive { get; set; }
        public bool IsAutoDelete { get; set; }

        public int ConsumerCount => messagePublished.Count;

        public void PublishMessage(RabbitMessage message)
        {
            var queueMessage = message.Copy();
            queueMessage.Queue = Name;
            Messages.Enqueue(queueMessage);
            foreach (var handler in messagePublished)
            {
                handler(this, queueMessage);
            }
        }

        public void ClearMessages()
        {
            while (Messages.TryDequeue(out _))
                ;
        }
    }
}