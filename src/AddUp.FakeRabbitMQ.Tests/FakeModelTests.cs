using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Framing;
using Xunit;

namespace AddUp.RabbitMQ.Fakes
{
    [ExcludeFromCodeCoverage]
    public class FakeModelTests
    {
        [Fact]
        public void CreateBasicProperties_ReturnsBasicProperties()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                // Act
                var result = model.CreateBasicProperties();

                // Assert
                Assert.NotNull(result);
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void ChannelFlow_SetsIfTheChannelIsActive(bool value)
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                // Act
                model.ChannelFlow(value);

                // Assert
                Assert.Equal(value, model.IsChannelFlowActive);
            }
        }

        [Fact]
        public void ExchangeDeclare_AllArguments_CreatesExchange()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                const string exchangeName = "someExchange";
                const string exchangeType = "someType";
                const bool isDurable = true;
                const bool isAutoDelete = false;
                var arguments = new Dictionary<string, object>();

                // Act
                model.ExchangeDeclare(exchange: exchangeName, type: exchangeType, durable: isDurable, autoDelete: isAutoDelete, arguments: arguments);

                // Assert
                Assert.Single(node.Exchanges);

                var exchange = node.Exchanges.First();
                AssertExchangeDetails(exchange, exchangeName, isAutoDelete, arguments, isDurable, exchangeType);
            }
        }

        [Fact]
        public void ExchangeDeclare_WithNameTypeAndDurable_CreatesExchange()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                const string exchangeName = "someExchange";
                const string exchangeType = "someType";
                const bool isDurable = true;

                // Act
                model.ExchangeDeclare(exchange: exchangeName, type: exchangeType, durable: isDurable);

                // Assert
                Assert.Single(node.Exchanges);

                var exchange = node.Exchanges.First();
                AssertExchangeDetails(exchange, exchangeName, false, null, isDurable, exchangeType);
            }
        }

        [Fact]
        public void ExchangeDeclare_WithNameType_CreatesExchange()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                const string exchangeName = "someExchange";
                const string exchangeType = "someType";

                // Act
                model.ExchangeDeclare(exchange: exchangeName, type: exchangeType);

                // Assert
                Assert.Single(node.Exchanges);

                var exchange = node.Exchanges.First();
                AssertExchangeDetails(exchange, exchangeName, false, null, false, exchangeType);
            }
        }

        [Fact]
        public void ExchangeDeclarePassive_WithName_CreatesExchange()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                const string exchangeName = "someExchange";

                // Act
                model.ExchangeDeclarePassive(exchange: exchangeName);

                // Assert
                Assert.Single(node.Exchanges);

                var exchange = node.Exchanges.First();
                AssertExchangeDetails(exchange, exchangeName, false, null, false, null);
            }
        }

        [Fact]
        public void ExchangeDeclareNoWait_CreatesExchange()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                const string exchangeName = "someExchange";
                const string exchangeType = "someType";
                const bool isDurable = true;
                const bool isAutoDelete = false;
                var arguments = new Dictionary<string, object>();

                // Act
                model.ExchangeDeclareNoWait(exchange: exchangeName, type: exchangeType, durable: isDurable, autoDelete: isAutoDelete, arguments: arguments);

                // Assert
                Assert.Single(node.Exchanges);

                var exchange = node.Exchanges.First();
                AssertExchangeDetails(exchange, exchangeName, isAutoDelete, arguments, isDurable, exchangeType);
            }
        }

        private static void AssertExchangeDetails(KeyValuePair<string, RabbitExchange> exchange, string exchangeName, bool isAutoDelete, IDictionary<string, object> arguments, bool isDurable, string exchangeType)
        {
            Assert.Equal(exchangeName, exchange.Key);
            Assert.Equal(isAutoDelete, exchange.Value.AutoDelete);
            Assert.Equal(arguments, exchange.Value.Arguments);
            Assert.Equal(isDurable, exchange.Value.IsDurable);
            Assert.Equal(exchangeName, exchange.Value.Name);
            Assert.Equal(exchangeType, exchange.Value.Type);
        }

        [Fact]
        public void ExchangeDelete_NameOnlyExchangeExists_RemovesTheExchange()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                const string exchangeName = "someExchange";
                model.ExchangeDeclare(exchangeName, "someType");

                // Act
                model.ExchangeDelete(exchange: exchangeName);

                // Assert
                Assert.Empty(node.Exchanges);
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void ExchangeDelete_ExchangeExists_RemovesTheExchange(bool ifUnused)
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                const string exchangeName = "someExchange";
                model.ExchangeDeclare(exchangeName, "someType");

                // Act
                model.ExchangeDelete(exchange: exchangeName, ifUnused: ifUnused);

                // Assert
                Assert.Empty(node.Exchanges);
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void ExchangeDeleteNoWait_ExchangeExists_RemovesTheExchange(bool ifUnused)
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                const string exchangeName = "someExchange";
                model.ExchangeDeclare(exchangeName, "someType");

                // Act
                model.ExchangeDeleteNoWait(exchange: exchangeName, ifUnused: ifUnused);

                // Assert
                Assert.Empty(node.Exchanges);
            }
        }

        [Fact]
        public void ExchangeDelete_ExchangeDoesNotExists_DoesNothing()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                const string exchangeName = "someExchange";
                model.ExchangeDeclare(exchangeName, "someType");

                // Act
                model.ExchangeDelete(exchange: "someOtherExchange");

                // Assert
                Assert.Single(node.Exchanges);
            }
        }

        [Fact]
        public void ExchangeBind_BindsAnExchangeToAQueue()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                const string queueName = "someQueue";
                const string exchangeName = "someExchange";
                const string routingKey = "someRoutingKey";
                var arguments = new Dictionary<string, object>();

                model.ExchangeDeclare(exchangeName, "direct");
                model.QueueDeclarePassive(queueName);

                // Act
                model.ExchangeBind(queueName, exchangeName, routingKey, arguments);

                // Assert
                AssertBinding(node, exchangeName, routingKey, queueName);
            }
        }

        [Fact]
        public void QueueBind_BindsAnExchangeToAQueue()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                const string queueName = "someQueue";
                const string exchangeName = "someExchange";
                const string routingKey = "someRoutingKey";
                var arguments = new Dictionary<string, object>();

                model.ExchangeDeclare(exchangeName, "direct");
                model.QueueDeclarePassive(queueName);

                // Act
                model.QueueBind(queueName, exchangeName, routingKey, arguments);

                // Assert
                AssertBinding(node, exchangeName, routingKey, queueName);
            }
        }

        private static void AssertBinding(RabbitServer server, string exchangeName, string routingKey, string queueName)
        {
            Assert.Single(server.Exchanges[exchangeName].Bindings);
            Assert.Equal(routingKey, server.Exchanges[exchangeName].Bindings.First().Value.RoutingKey);
            Assert.Equal(queueName, server.Exchanges[exchangeName].Bindings.First().Value.Queue.Name);
        }

        [Fact]
        public void ExchangeUnbind_RemovesBinding()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                const string queueName = "someQueue";
                const string exchangeName = "someExchange";
                const string routingKey = "someRoutingKey";
                var arguments = new Dictionary<string, object>();

                model.ExchangeDeclare(exchangeName, "direct");
                model.QueueDeclarePassive(queueName);
                model.ExchangeBind(exchangeName, queueName, routingKey, arguments);

                // Act
                model.ExchangeUnbind(queueName, exchangeName, routingKey, arguments);

                // Assert
                Assert.True(node.Exchanges[exchangeName].Bindings.IsEmpty);
                Assert.True(node.Queues[queueName].Bindings.IsEmpty);
            }
        }

        [Fact]
        public void QueueUnbind_RemovesBinding()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                const string queueName = "someQueue";
                const string exchangeName = "someExchange";
                const string routingKey = "someRoutingKey";
                var arguments = new Dictionary<string, object>();

                model.ExchangeDeclare(exchangeName, "direct");
                model.QueueDeclarePassive(queueName);
                model.ExchangeBind(exchangeName, queueName, routingKey, arguments);

                // Act
                model.QueueUnbind(queueName, exchangeName, routingKey, arguments);

                // Assert
                Assert.True(node.Exchanges[exchangeName].Bindings.IsEmpty);
                Assert.True(node.Queues[queueName].Bindings.IsEmpty);
            }
        }

        [Fact]
        public void QueueDeclare_NoArguments_CreatesQueue()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                // Act
                model.QueueDeclare();

                // Assert
                Assert.Single(node.Queues);
            }
        }

        [Fact]
        public void QueueDeclarePassive_WithName_CreatesQueue()
        {
            // Arrange
            var node = new RabbitServer();
            var model = new FakeModel(node);

            const string queueName = "myQueue";

            // Act
            model.QueueDeclarePassive(queueName);

            // Assert
            Assert.Single(node.Queues);
            Assert.Equal(queueName, node.Queues.First().Key);
            Assert.Equal(queueName, node.Queues.First().Value.Name);
        }

        [Fact]
        public void QueueDeclare_CreatesQueue()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                const string queueName = "someQueue";
                const bool isDurable = true;
                const bool isExclusive = true;
                const bool isAutoDelete = false;
                var arguments = new Dictionary<string, object>();

                // Act
                model.QueueDeclare(queue: queueName, durable: isDurable, exclusive: isExclusive, autoDelete: isAutoDelete, arguments: arguments);

                // Assert
                Assert.Single(node.Queues);

                var queue = node.Queues.First();
                AssertQueueDetails(queue, queueName, isAutoDelete, arguments, isDurable, isExclusive);
            }
        }

        private static void AssertQueueDetails(KeyValuePair<string, RabbitQueue> queue, string exchangeName, bool isAutoDelete, Dictionary<string, object> arguments, bool isDurable, bool isExclusive)
        {
            Assert.Equal(exchangeName, queue.Key);
            Assert.Equal(isAutoDelete, queue.Value.IsAutoDelete);
            Assert.Equal(arguments, queue.Value.Arguments);
            Assert.Equal(isDurable, queue.Value.IsDurable);
            Assert.Equal(exchangeName, queue.Value.Name);
            Assert.Equal(isExclusive, queue.Value.IsExclusive);
        }

        [Fact]
        public void QueueDelete_NameOnly_DeletesTheQueue()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                const string queueName = "someName";
                model.QueueDeclare(queueName, true, true, true, null);

                // Act
                model.QueueDelete(queueName);

                // Assert
                Assert.True(node.Queues.IsEmpty);
            }
        }

        [Fact]
        public void QueueDelete_WithArguments_DeletesTheQueue()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                const string queueName = "someName";
                model.QueueDeclare(queueName, true, true, true, null);

                // Act
                model.QueueDelete(queueName, true, true);

                // Assert
                Assert.True(node.Queues.IsEmpty);
            }
        }

        [Fact]
        public void QueueDeleteNoWait_WithArguments_DeletesTheQueue()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                const string queueName = "someName";
                model.QueueDeclare(queueName, true, true, true, null);

                // Act
                model.QueueDeleteNoWait(queueName, true, true);

                // Assert
                Assert.True(node.Queues.IsEmpty);
            }
        }

        [Fact]
        public void QueueDelete_NonExistentQueue_DoesNothing()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                // Act
                model.QueueDelete("someQueue");

                // Assert
                Assert.True(node.Queues.IsEmpty);
            }
        }

        [Fact]
        public void QueuePurge_RemovesAllMessagesFromQueue()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                model.QueueDeclarePassive("my_other_queue");
                node.Queues["my_other_queue"].Messages.Enqueue(new RabbitMessage());
                node.Queues["my_other_queue"].Messages.Enqueue(new RabbitMessage());

                model.QueueDeclarePassive("my_queue");
                node.Queues["my_queue"].Messages.Enqueue(new RabbitMessage());
                node.Queues["my_queue"].Messages.Enqueue(new RabbitMessage());
                node.Queues["my_queue"].Messages.Enqueue(new RabbitMessage());
                node.Queues["my_queue"].Messages.Enqueue(new RabbitMessage());

                // Act
                model.QueuePurge("my_queue");

                // Assert
                Assert.True(node.Queues["my_queue"].Messages.IsEmpty);
                Assert.False(node.Queues["my_other_queue"].Messages.IsEmpty);
            }
        }

        [Fact]
        public void Close_ClosesTheChannel()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                // Act
                model.Close();

                // Assert
                Assert.True(model.IsClosed);
                Assert.False(model.IsOpen);
                Assert.NotNull(model.CloseReason);
            }
        }

        [Fact]
        public void Close_WithArguments_ClosesTheChannel()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                // Act
                model.Close(5, "some message");

                // Assert
                Assert.True(model.IsClosed);
                Assert.False(model.IsOpen);
                Assert.NotNull(model.CloseReason);
            }
        }

        [Fact]
        public void Abort_ClosesTheChannel()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                // Act
                model.Abort();

                // Assert
                Assert.True(model.IsClosed);
                Assert.False(model.IsOpen);
                Assert.NotNull(model.CloseReason);
            }
        }

        [Fact]
        public void Abort_WithArguments_ClosesTheChannel()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                // Act
                model.Abort(5, "some message");

                // Assert
                Assert.True(model.IsClosed);
                Assert.False(model.IsOpen);
                Assert.NotNull(model.CloseReason);
            }
        }

        [Fact]
        public void BasicPublish_PublishesMessage()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                model.ExchangeDeclare("my_exchange", ExchangeType.Direct);
                model.QueueDeclarePassive("my_queue");
                model.ExchangeBind("my_queue", "my_exchange", null);

                var message = "hello world!";
                var encodedMessage = Encoding.ASCII.GetBytes(message);

                // Act
                model.BasicPublish("my_exchange", null, new BasicProperties(), encodedMessage);

                // Assert
                Assert.Single(node.Queues["my_queue"].Messages);
                Assert.Equal(encodedMessage, node.Queues["my_queue"].Messages.First().Body);
            }
        }

        [Fact]
        public void BasicPublishBatch_PublishesMessage()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                model.ExchangeDeclare("my_exchange", ExchangeType.Direct);
                model.QueueDeclarePassive("my_queue");
                model.ExchangeBind("my_queue", "my_exchange", null);

                var messages = new[] { "hello world!", "Thank you, @inbarbarkai" };
                var encodedMessages = messages.Select(m => Encoding.ASCII.GetBytes(m)).ToArray();

                // Act
                var batch = model.CreateBasicPublishBatch();
                batch.Add("my_exchange", null, true, new BasicProperties(), encodedMessages[0]);
                batch.Add("my_exchange", null, true, new BasicProperties(), encodedMessages[1]);
                batch.Publish();

                // Assert
                Assert.Equal(2, node.Queues["my_queue"].Messages.Count);

                var index = 0;
                foreach (var item in node.Queues["my_queue"].Messages)
                {
                    Assert.Equal(encodedMessages[index], item.Body);
                    index++;
                }
            }
        }

        [Fact]
        public void BasicAck()
        {
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                model.ExchangeDeclare("my_exchange", ExchangeType.Direct);
                model.QueueDeclarePassive("my_queue");
                model.ExchangeBind("my_queue", "my_exchange", null);

                var message = "hello world!";
                var encodedMessage = Encoding.ASCII.GetBytes(message);
                model.BasicPublish("my_exchange", null, new BasicProperties(), encodedMessage);

                var consumer = new EventingBasicConsumer(model);
                model.BasicConsume("my_queue", false, consumer);
                Assert.True(consumer.IsRunning);

                // Act
                var deliveryTag = model.WorkingMessages.First().Key;
                model.BasicAck(deliveryTag, false);

                // Assert
                Assert.Empty(node.Queues["my_queue"].Messages);
            }
        }

        [Fact]
        public void BasicCancel()
        {
            // Assert
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                model.QueueDeclarePassive("my_queue");
                var expectedConsumerTag = "foo";
                var actualConsumerTag = "";

                // Act
                var consumer = new EventingBasicConsumer(model) { ConsumerTag = expectedConsumerTag };
                consumer.Unregistered += (s, e) => actualConsumerTag = e.ConsumerTag;

                model.BasicConsume("my_queue", false, expectedConsumerTag, consumer);
                Assert.True(consumer.IsRunning);
                model.BasicCancel(expectedConsumerTag);
                Assert.False(consumer.IsRunning);

                // Assert
                Assert.Equal(expectedConsumerTag, actualConsumerTag);
            }
        }

        [Fact]
        public void BasicGet_MessageOnQueue_GetsMessage()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                model.ExchangeDeclare("my_exchange", ExchangeType.Direct);
                model.QueueDeclarePassive("my_queue");
                model.ExchangeBind("my_queue", "my_exchange", null);

                var message = "hello world!";
                var encodedMessage = Encoding.ASCII.GetBytes(message);
                model.BasicPublish("my_exchange", null, new BasicProperties(), encodedMessage);

                // Act
                var response = model.BasicGet("my_queue", false);

                // Assert
                Assert.Equal(encodedMessage, response.Body);
                Assert.True(response.DeliveryTag > 0ul);
            }
        }

        [Fact]
        public void BasicGet_NoMessageOnQueue_ReturnsNull()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                model.QueueDeclarePassive("my_queue");

                // Act
                var response = model.BasicGet("my_queue", false);

                // Assert
                Assert.Null(response);
            }
        }

        [Fact]
        public void BasicGet_NoQueue_ReturnsNull()
        {
            // Arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                // Act
                var response = model.BasicGet("my_queue", false);

                // Assert
                Assert.Null(response);
            }
        }

        [Theory]
        [InlineData(true, 1)] // If requeue param to BasicNack is true, the message that is nacked should remain in Rabbit
        [InlineData(false, 0)] // If requeue param to BasicNack is false, the message that is nacked should be removed from Rabbit
        public void Nacking_Message_Should_Not_Reenqueue_Brand_New_Message(bool requeue, int expectedMessageCount)
        {
            // arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                model.ExchangeDeclare("my_exchange", ExchangeType.Direct);
                model.QueueDeclare("my_queue");
                model.ExchangeBind("my_queue", "my_exchange", null);

                var encodedMessage = Encoding.ASCII.GetBytes("hello world!");
                model.BasicPublish("my_exchange", null, new BasicProperties(), encodedMessage);

                var consumer = new EventingBasicConsumer(model);
                model.BasicConsume("my_queue", false, consumer);
                Assert.True(consumer.IsRunning);

                // act
                var deliveryTag = model.WorkingMessages.First().Key;
                model.BasicNack(deliveryTag, false, requeue);

                // assert
                Assert.Equal(expectedMessageCount, node.Queues["my_queue"].Messages.Count);
                Assert.Equal(expectedMessageCount, model.WorkingMessages.Count);
            }
        }

        [Theory]
        [InlineData(true, 0)] // BasicGet WITH auto-ack SHOULD remove the message from the queue
        [InlineData(false, 1)] // BasicGet with NO auto-ack should NOT remove the message from the queue
        public void BasicGet_Should_Not_Remove_The_Message_From_Queue_If_Not_Acked(bool autoAck, int expectedMessageCount)
        {
            // arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                model.ExchangeDeclare("my_exchange", ExchangeType.Direct);
                model.QueueDeclare("my_queue");
                model.ExchangeBind("my_queue", "my_exchange", null);

                var encodedMessage = Encoding.ASCII.GetBytes("hello world!");
                model.BasicPublish("my_exchange", null, new BasicProperties(), encodedMessage);

                // act
                _ = model.BasicGet("my_queue", autoAck);

                // assert
                Assert.Equal(expectedMessageCount, node.Queues["my_queue"].Messages.Count);
                Assert.Equal(expectedMessageCount, model.WorkingMessages.Count);
            }
        }

        [Fact]
        public void WaitForConfirms_should_throw_if_ConfirmSelect_was_not_called()
        {
            // arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
                // act & assert
                Assert.Throws<InvalidOperationException>(() => model.WaitForConfirms());
        }

        [Fact]
        public void WaitForConfirmsOrDie_returns_true_if_ConfirmSelect_was_called()
        {
            // arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                // act
                model.ConfirmSelect();
                var result = model.WaitForConfirms();

                // assert
                Assert.True(result);
            }
        }

        [Fact]
        public void WaitForConfirmsOrDie_should_throw_if_ConfirmSelect_was_not_called()
        {
            // arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
                // act & assert
                Assert.Throws<InvalidOperationException>(() => model.WaitForConfirmsOrDie());
        }

        [Fact]
        public void WaitForConfirmsOrDie_does_not_throw_if_ConfirmSelect_was_called()
        {
            // arrange
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                // act
                model.ConfirmSelect();
                model.WaitForConfirmsOrDie();
            }

            // assert
            Assert.True(true);
        }

        [Fact]
        public void BasicConsume_works_for_asynchronous_consumers()
        {
            // Assert
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                model.ExchangeDeclare("my_exchange", ExchangeType.Direct);
                model.QueueDeclarePassive("my_queue");
                model.ExchangeBind("my_queue", "my_exchange", null);

                var message = "hello world!";
                var encodedMessage = Encoding.ASCII.GetBytes(message);
                model.BasicPublish("my_exchange", null, new BasicProperties(), encodedMessage);

                // Act
                var consumer = new FakeAsyncDefaultBasicConsumer(model);
                model.BasicConsume("my_queue", false, consumer);
                Assert.True(consumer.IsRunning);

                var deliveredPayload = consumer.LastDelivery.body;

                // Assert
                Assert.Equal(encodedMessage, deliveredPayload);
            }
        }

        [Fact]
        public void BasicCancel_works_for_asynchronous_consumers()
        {
            // Assert
            var node = new RabbitServer();
            using (var model = new FakeModel(node))
            {
                model.QueueDeclarePassive("my_queue");
                var expectedConsumerTag = "foo";

                // Act
                var consumer = new FakeAsyncDefaultBasicConsumer(model);

                model.BasicConsume("my_queue", false, expectedConsumerTag, consumer);
                Assert.True(consumer.IsRunning);
                model.BasicCancel(expectedConsumerTag);
                Assert.False(consumer.IsRunning);

                // Assert
                Assert.Equal(expectedConsumerTag, consumer.LastCancelOkConsumerTag);
            }
        }
    }
}