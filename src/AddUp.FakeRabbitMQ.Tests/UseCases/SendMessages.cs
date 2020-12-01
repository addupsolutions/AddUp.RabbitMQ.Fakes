using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using RabbitMQ.Client;
using Xunit;

namespace AddUp.RabbitMQ.Fakes.UseCases
{
    [ExcludeFromCodeCoverage]
    public class SendMessages
    {
        [Fact]
        public void SendToExchangeOnly()
        {
            var rabbitServer = new RabbitServer();
            var connectionFactory = new FakeConnectionFactory(rabbitServer);

            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                const string message = "hello world!";
                var messageBody = Encoding.ASCII.GetBytes(message);
                channel.BasicPublish(exchange: "my_exchange", routingKey: null, mandatory: false, basicProperties: null, body: messageBody);
            }

            Assert.Single(rabbitServer.Exchanges["my_exchange"].Messages);
        }

        [Fact]
        public void SendToExchangeWithBoundQueue()
        {
            var rabbitServer = new RabbitServer();
            var connectionFactory = new FakeConnectionFactory(rabbitServer);

            ConfigureQueueBinding(rabbitServer, "my_exchange", "some_queue");

            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                const string message = "hello world!";
                var messageBody = Encoding.ASCII.GetBytes(message);
                channel.BasicPublish(exchange: "my_exchange", routingKey: null, mandatory: false, basicProperties: null, body: messageBody);
            }

            Assert.Single(rabbitServer.Queues["some_queue"].Messages);
        }

        [Fact]
        public void SendToExchangeWithMultipleBoundQueues()
        {
            var rabbitServer = new RabbitServer();
            var connectionFactory = new FakeConnectionFactory(rabbitServer);

            ConfigureQueueBinding(rabbitServer, "my_exchange", "some_queue");
            ConfigureQueueBinding(rabbitServer, "my_exchange", "some_other_queue");

            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                const string message = "hello world!";
                var messageBody = Encoding.ASCII.GetBytes(message);
                channel.BasicPublish(exchange: "my_exchange", routingKey: null, mandatory: false, basicProperties: null, body: messageBody);
            }

            Assert.Single(rabbitServer.Queues["some_queue"].Messages);
            Assert.Single(rabbitServer.Queues["some_other_queue"].Messages);
        }

        [Fact]
        public void SendToExchangeWithAlternate()
        {
            var rabbitServer = new RabbitServer();
            var connectionFactory = new FakeConnectionFactory(rabbitServer);

            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "my_exchange", type: ExchangeType.Topic, arguments: new Dictionary<string, object> { ["alternate-exchange"] = "my_alternate_exchange" });
                channel.ExchangeDeclare(exchange: "my_alternate_exchange", type: ExchangeType.Fanout);
                channel.QueueDeclare(queue: "main_queue", durable: false, exclusive: false, autoDelete: false, arguments: null);
                channel.QueueDeclare(queue: "fallback_queue", durable: false, exclusive: false, autoDelete: false, arguments: null);

                channel.QueueBind("fallback_queue", "my_alternate_exchange", null);
                channel.QueueBind("main_queue", "my_exchange", "topic");
            }

            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                const string message = "hello world!";
                var messageBody = Encoding.ASCII.GetBytes(message);
                channel.BasicPublish(exchange: "my_exchange", routingKey: "other_topic", mandatory: false, basicProperties: null, body: messageBody);

                messageBody = Encoding.ASCII.GetBytes(message);
                channel.BasicPublish(exchange: "my_exchange", routingKey: "topic", mandatory: false, basicProperties: null, body: messageBody);
            }

            Assert.Equal(2, rabbitServer.Exchanges["my_exchange"].Messages.Count);
            Assert.Single(rabbitServer.Exchanges["my_alternate_exchange"].Messages);
            Assert.Single(rabbitServer.Queues["fallback_queue"].Messages);
            Assert.Single(rabbitServer.Queues["main_queue"].Messages);
        }

        private void ConfigureQueueBinding(RabbitServer rabbitServer, string exchangeName, string queueName)
        {
            var connectionFactory = new FakeConnectionFactory(rabbitServer);
            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);
                channel.ExchangeDeclare(exchange: exchangeName, type: ExchangeType.Direct);

                channel.QueueBind(queueName, exchangeName, null);
            }
        }
    }
}