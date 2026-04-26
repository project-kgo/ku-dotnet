using Microsoft.EntityFrameworkCore;

namespace Ku.Utils.Database.PostgreSql;

internal sealed class PostgreSqlDbContext(DbContextOptions<PostgreSqlDbContext> options) : DbContext(options);
