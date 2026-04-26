using Ku.Utils.Database.PostgreSql;

namespace Ku.Utils.Tests;

public sealed class PostgreSqlDataSourceFactoryTests
{
    [Fact]
    public void GetOrCreate_WhenOptionsAreEquivalent_ReturnsSameDataSource()
    {
        var first = PostgreSqlDataSourceFactory.GetOrCreate(new PostgreSqlConnectionOptions
        {
            ConnectionString = "Host=localhost;Database=ku_test;Username=ku;Password=secret"
        });
        var second = PostgreSqlDataSourceFactory.GetOrCreate(new PostgreSqlConnectionOptions
        {
            ConnectionString = "Password=secret;Username=ku;Database=ku_test;Host=localhost"
        });

        Assert.Same(first, second);
    }

    [Fact]
    public void GetOrCreate_WhenConnectionStringIsDifferent_ReturnsDifferentDataSource()
    {
        var first = PostgreSqlDataSourceFactory.GetOrCreate(new PostgreSqlConnectionOptions
        {
            ConnectionString = "Host=localhost;Database=ku_test_a;Username=ku;Password=secret"
        });
        var second = PostgreSqlDataSourceFactory.GetOrCreate(new PostgreSqlConnectionOptions
        {
            ConnectionString = "Host=localhost;Database=ku_test_b;Username=ku;Password=secret"
        });

        Assert.NotSame(first, second);
    }

    [Fact]
    public async Task GetOrCreate_WhenCalledConcurrently_ReturnsSameDataSource()
    {
        var options = new PostgreSqlConnectionOptions
        {
            ConnectionString = "Host=localhost;Database=ku_concurrent;Username=ku;Password=secret"
        };

        var tasks = Enumerable.Range(0, 32)
            .Select(_ => Task.Run(() => PostgreSqlDataSourceFactory.GetOrCreate(options)))
            .ToArray();

        var dataSources = await Task.WhenAll(tasks);

        Assert.All(dataSources, dataSource => Assert.Same(dataSources[0], dataSource));
    }

    [Fact]
    public void GetOrCreate_WhenTypedOptionsAreEquivalent_ReturnsSameDataSource()
    {
        var first = PostgreSqlDataSourceFactory.GetOrCreate(new PostgreSqlConnectionOptions
        {
            ConnectionString = "Host=localhost;Database=ku_pool;Username=ku;Password=secret",
            Pooling = true,
            MinimumPoolSize = 2,
            MaximumPoolSize = 20,
            Timeout = TimeSpan.FromSeconds(3),
            CommandTimeout = TimeSpan.FromSeconds(10),
            ConnectionIdleLifetime = TimeSpan.FromMinutes(5),
            ConnectionPruningInterval = TimeSpan.FromSeconds(15),
            ConnectionLifetime = TimeSpan.FromMinutes(30)
        });
        var second = PostgreSqlDataSourceFactory.GetOrCreate(new PostgreSqlConnectionOptions
        {
            ConnectionString = """
                Host=localhost;
                Database=ku_pool;
                Username=ku;
                Password=secret;
                Pooling=true;
                Minimum Pool Size=2;
                Maximum Pool Size=20;
                Timeout=3;
                Command Timeout=10;
                Connection Idle Lifetime=300;
                Connection Pruning Interval=15;
                Connection Lifetime=1800
                """
        });

        Assert.Same(first, second);
    }

    [Fact]
    public void GetOrCreate_WhenTypedOptionOverridesConnectionString_ReturnsDifferentDataSource()
    {
        var fromConnectionString = PostgreSqlDataSourceFactory.GetOrCreate(new PostgreSqlConnectionOptions
        {
            ConnectionString = "Host=localhost;Database=ku_override;Username=ku;Password=secret;Maximum Pool Size=20"
        });
        var fromTypedOption = PostgreSqlDataSourceFactory.GetOrCreate(new PostgreSqlConnectionOptions
        {
            ConnectionString = "Host=localhost;Database=ku_override;Username=ku;Password=secret;Maximum Pool Size=20",
            MaximumPoolSize = 30
        });

        Assert.NotSame(fromConnectionString, fromTypedOption);
    }

    [Fact]
    public void GetOrCreate_WhenMinimumPoolSizeIsGreaterThanMaximumPoolSize_Throws()
    {
        var options = new PostgreSqlConnectionOptions
        {
            ConnectionString = "Host=localhost;Database=ku_invalid;Username=ku;Password=secret",
            MinimumPoolSize = 10,
            MaximumPoolSize = 5
        };

        Assert.Throws<ArgumentException>(() => PostgreSqlDataSourceFactory.GetOrCreate(options));
    }
}
