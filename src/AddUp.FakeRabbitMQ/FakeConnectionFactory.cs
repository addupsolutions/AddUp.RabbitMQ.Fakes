﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace AddUp.RabbitMQ.Fakes
{
    public sealed class FakeConnectionFactory : IConnectionFactory
    {
        public FakeConnectionFactory() : this(new RabbitServer()) { }
        public FakeConnectionFactory(RabbitServer rabbitServer) => Server = rabbitServer ?? throw new ArgumentNullException(nameof(rabbitServer));

        public IConnection Connection => UnderlyingConnection;

        public RabbitServer Server { get; private set; }                
        public IDictionary<string, object> ClientProperties { get; set; }
        public string Password { get; set; }
        public ushort RequestedChannelMax { get; set; }
        public uint RequestedFrameMax { get; set; }
        public ushort RequestedHeartbeat { get; set; }
        public bool UseBackgroundThreadsForIO { get; set; }
        public string UserName { get; set; }
        public string VirtualHost { get; set; }
        public Uri Uri { get; set; }
        public TaskScheduler TaskScheduler { get; set; }
        public TimeSpan HandshakeContinuationTimeout { get; set; }
        public TimeSpan ContinuationTimeout { get; set; }
        
        private FakeConnection UnderlyingConnection { get; set; }

        public AuthMechanismFactory AuthMechanismFactory(IList<string> mechanismNames) => new PlainMechanismFactory();

        public IConnection CreateConnection() => CreateConnection("");
        public IConnection CreateConnection(IList<string> hostnames) => CreateConnection(hostnames, "");
        public IConnection CreateConnection(IList<string> hostnames, string clientProvidedName) => CreateConnection(clientProvidedName);
        public IConnection CreateConnection(IList<AmqpTcpEndpoint> endpoints) => CreateConnection("");
        public IConnection CreateConnection(string clientProvidedName)
        {
            if (UnderlyingConnection == null)
                UnderlyingConnection = new FakeConnection(Server, clientProvidedName);
            else
                UnderlyingConnection.ForceOpen();

            return Connection;
        }
    }
}