using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace AddUp.RabbitMQ.Fakes
{
    internal sealed class RabbitExchange
    {
        private readonly IBindingMatcher matcher;
        private readonly RabbitServer server;

        public RabbitExchange(string type, RabbitServer server)
        {
            Type = type;
            this.server = server;
            matcher = BindingMatcherFactory.Create(type);

            Messages = new ConcurrentQueue<RabbitMessage>();
            Bindings = new ConcurrentDictionary<string, RabbitExchangeQueueBinding>();
            Arguments = new Dictionary<string, object>();
        }

        public string Type { get; set; }
        public ConcurrentQueue<RabbitMessage> Messages { get; }
        public ConcurrentDictionary<string, RabbitExchangeQueueBinding> Bindings { get; }
        public IDictionary<string, object> Arguments { get; set; }
        public string Name { get; set; }
        public bool IsDurable { get; set; }
        public bool AutoDelete { get; set; }

        public void PublishMessage(RabbitMessage message)
        {
            Messages.Enqueue(message);

            IEnumerable<RabbitExchangeQueueBinding> matchingBindings;
            if (string.IsNullOrWhiteSpace(message.RoutingKey))
            {
                matchingBindings = this.Bindings.Values;
            }
            else
            {
                matchingBindings = Bindings.Values.Where(b => matcher.Matches(message.RoutingKey, b.RoutingKey));
            }
            foreach (var binding in matchingBindings)
                binding.Queue.PublishMessage(message);

            if (!matchingBindings.Any()
                && Arguments != null
                && Arguments.TryGetValue("alternate-exchange", out var alternateExchangeName)
                && server.Exchanges.TryGetValue(alternateExchangeName.ToString(), out var alternateExchange))
            {
                alternateExchange.PublishMessage(message);
            }
        }
    }
}