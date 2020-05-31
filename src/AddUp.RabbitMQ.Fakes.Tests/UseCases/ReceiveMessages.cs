﻿using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using FluentAssertions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Framing;
using Xunit;

namespace AddUp.RabbitMQ.Fakes.UseCases
{
    [ExcludeFromCodeCoverage]
    public class ReceiveMessages
    {
        [Fact]
        public void ReceiveMessagesOnQueue()
        {
            var rabbitServer = new RabbitServer();

            ConfigureQueueBinding(rabbitServer, "my_exchange", "my_queue");
            SendMessage(rabbitServer,"my_exchange","hello_world");

            var connectionFactory = new FakeConnectionFactory(rabbitServer);
            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                // First message
                var message = channel.BasicGet("my_queue", autoAck: false);
                
                Assert.NotNull(message);
                var messageBody = Encoding.ASCII.GetString(message.Body.ToArray());

                Assert.Equal("hello_world", messageBody);

                channel.BasicAck(message.DeliveryTag,multiple:false);
            }
        }

        [Fact]
        public void ReceiveMessagesOnQueueWithBasicProperties()
        {
            var rabbitServer = new RabbitServer();

            ConfigureQueueBinding(rabbitServer, "my_exchange", "my_queue");
            var basicProperties = new FakeBasicProperties
            {
                Headers = new Dictionary<string, object>() {{"TestKey", "TestValue"}},
                CorrelationId = Guid.NewGuid().ToString(),
                ReplyTo = "TestQueue",
                Timestamp = new AmqpTimestamp(123456),
                ReplyToAddress = new PublicationAddress("exchangeType", "excahngeName", "routingKey"),
                ClusterId = "1",
                ContentEncoding = "encoding",
                ContentType = "type",
                DeliveryMode = 1,
                Expiration = "none",
                MessageId = "id",
                Priority = 1,
                Type = "type",
                UserId = "1",
                AppId = "1"
            };

            SendMessage(rabbitServer, "my_exchange", "hello_world", basicProperties);
            var connectionFactory = new FakeConnectionFactory(rabbitServer);
            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                // First message
                var message = channel.BasicGet("my_queue", autoAck: false);

                Assert.NotNull(message);
                var messageBody = Encoding.ASCII.GetString(message.Body.ToArray());

                Assert.Equal("hello_world", messageBody);

                var actualBasicProperties = message.BasicProperties;

                actualBasicProperties.Should().BeEquivalentTo(basicProperties);

                channel.BasicAck(message.DeliveryTag, multiple: false);
            }
        }

        [Fact]
        public void QueueingConsumer_MessagesOnQueueBeforeConsumerIsCreated_ReceiveMessagesOnQueue()
        {
            var rabbitServer = new RabbitServer();

            ConfigureQueueBinding(rabbitServer, "my_exchange", "my_queue");
            SendMessage(rabbitServer, "my_exchange", "hello_world");

            var connectionFactory = new FakeConnectionFactory(rabbitServer);
            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                var consumer = new QueueingBasicConsumer(channel);
                channel.BasicConsume("my_queue", false, consumer);

                if (consumer.Queue.Dequeue(TimeSpan.FromSeconds(5), out var messageOut))
                {
                    var messageBody = Encoding.ASCII.GetString(messageOut.Body.ToArray());
                    Assert.Equal("hello_world", messageBody);
                    channel.BasicAck(messageOut.DeliveryTag, multiple: false);
                }

                Assert.NotNull(messageOut);
            }
        }

        [Fact]
        public void QueueingConsumer_MessagesSentAfterConsumerIsCreated_ReceiveMessagesOnQueue()
        {
            var rabbitServer = new RabbitServer();

            ConfigureQueueBinding(rabbitServer, "my_exchange", "my_queue");
           
            var connectionFactory = new FakeConnectionFactory(rabbitServer);
            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                var consumer = new QueueingBasicConsumer(channel);
                channel.BasicConsume("my_queue", false, consumer);

                SendMessage(rabbitServer, "my_exchange", "hello_world");

                if (consumer.Queue.Dequeue(TimeSpan.FromSeconds(5), out var messageOut))
                {
                    var messageBody = Encoding.ASCII.GetString(messageOut.Body.ToArray());
                    Assert.Equal("hello_world", messageBody);
                    channel.BasicAck(messageOut.DeliveryTag, multiple: false);
                }

                Assert.NotNull(messageOut);
            }
        }

        private static void SendMessage(RabbitServer rabbitServer, string exchange, string message, IBasicProperties basicProperties = null)
        {
            var connectionFactory = new FakeConnectionFactory(rabbitServer);

            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                var messageBody = Encoding.ASCII.GetBytes(message);
                channel.BasicPublish(exchange: exchange, routingKey: null, mandatory: false, basicProperties: basicProperties, body: messageBody);
            }
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