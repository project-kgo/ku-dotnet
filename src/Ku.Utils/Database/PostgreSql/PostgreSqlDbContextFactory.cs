using Microsoft.EntityFrameworkCore;
using Npgsql;

namespace Ku.Utils.Database.PostgreSql;

internal static class PostgreSqlDbContextFactory
{
    public static PostgreSqlDbContext Create(NpgsqlDataSource dataSource)
    {
        var options = new DbContextOptionsBuilder<PostgreSqlDbContext>()
            .UseNpgsql(dataSource)
            .Options;

        return new PostgreSqlDbContext(options);
    }
}
