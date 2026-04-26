using System.Reflection;
using Ku.Utils.Database.Redis;
using StackExchange.Redis;

namespace Ku.Utils.Tests;

public sealed class RedisConnectionFactoryTests : IDisposable
{
    public RedisConnectionFactoryTests()
    {
        RedisConnectionFactory.ResetForTests();
        RedisConnectionFactory.ConnectionFactory = _ => RedisConnectionMultiplexerProxy.Create();
    }

    public void Dispose()
    {
        RedisConnectionFactory.ResetForTests();
    }

    [Fact]
    public void GetOrCreate_WhenOptionsAreEquivalent_ReturnsSameConnection()
    {
        var first = RedisConnectionFactory.GetOrCreate(new RedisConnectionOptions
        {
            ConnectionString = "localhost:6379,password=secret,abortConnect=false,connectTimeout=1"
        });
        var second = RedisConnectionFactory.GetOrCreate(new RedisConnectionOptions
        {
            ConnectionString = "connectTimeout=1,abortConnect=false,password=secret,localhost:6379"
        });

        Assert.Same(first, second);
    }

    [Fact]
    public void GetOrCreate_WhenConnectionStringIsDifferent_ReturnsDifferentConnection()
    {
        var first = RedisConnectionFactory.GetOrCreate(new RedisConnectionOptions
        {
            ConnectionString = "localhost:6379,defaultDatabase=1,abortConnect=false,connectTimeout=1"
        });
        var second = RedisConnectionFactory.GetOrCreate(new RedisConnectionOptions
        {
            ConnectionString = "localhost:6379,defaultDatabase=2,abortConnect=false,connectTimeout=1"
        });

        Assert.NotSame(first, second);
    }

    [Fact]
    public async Task GetOrCreate_WhenCalledConcurrently_ReturnsSameConnection()
    {
        var options = new RedisConnectionOptions
        {
            ConnectionString = "localhost:6379,abortConnect=false,connectTimeout=1,syncTimeout=1,asyncTimeout=1"
        };

        var tasks = Enumerable.Range(0, 32)
            .Select(_ => Task.Run(() => RedisConnectionFactory.GetOrCreate(options)))
            .ToArray();

        var connections = await Task.WhenAll(tasks);

        Assert.All(connections, connection => Assert.Same(connections[0], connection));
    }

    [Fact]
    public void GetOrCreate_WhenTypedOptionsAreEquivalent_ReturnsSameConnection()
    {
        var first = RedisConnectionFactory.GetOrCreate(new RedisConnectionOptions
        {
            ConnectionString = "localhost:6381",
            Pooling = true,
            ConnectTimeout = TimeSpan.FromMilliseconds(1),
            SyncTimeout = TimeSpan.FromMilliseconds(2),
            AsyncTimeout = TimeSpan.FromMilliseconds(3),
            KeepAlive = TimeSpan.FromSeconds(4),
            ConnectRetry = 0,
            AbortOnConnectFail = false,
            DefaultDatabase = 5,
            ClientName = "ku-test"
        });
        var second = RedisConnectionFactory.GetOrCreate(new RedisConnectionOptions
        {
            ConnectionString = "localhost:6381,pooling=True,connectTimeout=1,syncTimeout=2,asyncTimeout=3,keepAlive=4,connectRetry=0,abortConnect=false,defaultDatabase=5,name=ku-test"
        });

        Assert.Same(first, second);
    }

    [Fact]
    public void GetOrCreate_WhenTypedOptionOverridesConnectionString_ReturnsDifferentConnection()
    {
        var fromConnectionString = RedisConnectionFactory.GetOrCreate(new RedisConnectionOptions
        {
            ConnectionString = "localhost:6382,abortConnect=false,connectTimeout=1,syncTimeout=1,asyncTimeout=1,defaultDatabase=1"
        });
        var fromTypedOption = RedisConnectionFactory.GetOrCreate(new RedisConnectionOptions
        {
            ConnectionString = "localhost:6382,abortConnect=false,connectTimeout=1,syncTimeout=1,asyncTimeout=1,defaultDatabase=1",
            DefaultDatabase = 2
        });

        Assert.NotSame(fromConnectionString, fromTypedOption);
    }

    [Fact]
    public void GetOrCreate_WhenConnectionStringIsEmpty_Throws()
    {
        var options = new RedisConnectionOptions
        {
            ConnectionString = " "
        };

        Assert.Throws<ArgumentException>(() => RedisConnectionFactory.GetOrCreate(options));
    }

    [Fact]
    public void GetOrCreate_WhenConnectTimeoutIsNegative_Throws()
    {
        var options = new RedisConnectionOptions
        {
            ConnectionString = "localhost:6379",
            ConnectTimeout = TimeSpan.FromMilliseconds(-1)
        };

        Assert.Throws<ArgumentOutOfRangeException>(() => RedisConnectionFactory.GetOrCreate(options));
    }

    [Fact]
    public void GetOrCreate_WhenTypedNumericOptionsAreInvalid_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => RedisConnectionFactory.GetOrCreate(new RedisConnectionOptions
        {
            ConnectionString = "localhost:6379",
            KeepAlive = TimeSpan.FromSeconds(-1)
        }));
        Assert.Throws<ArgumentOutOfRangeException>(() => RedisConnectionFactory.GetOrCreate(new RedisConnectionOptions
        {
            ConnectionString = "localhost:6379",
            ConnectRetry = -1
        }));
        Assert.Throws<ArgumentOutOfRangeException>(() => RedisConnectionFactory.GetOrCreate(new RedisConnectionOptions
        {
            ConnectionString = "localhost:6379",
            DefaultDatabase = -1
        }));
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1852:密封内部类型", Justification = "DispatchProxy 要求代理类型不能是 sealed。")]
    private class RedisConnectionMultiplexerProxy : DispatchProxy
    {
        public static IConnectionMultiplexer Create()
        {
            return Create<IConnectionMultiplexer, RedisConnectionMultiplexerProxy>();
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            throw new NotSupportedException("测试替身不支持调用 Redis 连接方法。");
        }
    }
}
