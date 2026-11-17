using Microsoft.Extensions.Options;

namespace LinePulse.Core.Storage;

/// <summary>
/// Picks the storage provider from configuration.
/// </summary>
/// <remarks>
/// <para>
/// SQLite is the lab provider: no server, no container, one file that can be copied
/// off a machine and inspected. MSSQL is the plant provider, and the schema for it
/// is written and reviewable in <c>db/mssql/schema.sql</c>.
/// </para>
/// <para>
/// The MSSQL repository implementation is deliberately absent rather than
/// half-written. Nothing on this machine can run SQL Server -- Docker is off and an
/// 8 GB laptop will not host it -- so an implementation here would be untested code
/// claiming to work. Failing loudly at startup with a pointer to the schema is the
/// honest version: the seam is real, the SQL is real, and what has not been
/// executed says so.
/// </para>
/// </remarks>
public static class StorageRegistration
{
    public const string Sqlite = "Sqlite";
    public const string SqlServer = "SqlServer";

    public static void AddStorage(IServiceCollection services, IConfiguration configuration)
    {
        var provider = configuration.GetSection(StorageOptions.Section)["Provider"] ?? Sqlite;

        if (string.Equals(provider, SqlServer, StringComparison.OrdinalIgnoreCase))
        {
            throw new NotSupportedException(
                "Storage:Provider=SqlServer is not implemented in this build. The MSSQL schema is in "
                + "db/mssql/schema.sql and has been written but not executed -- this machine cannot run "
                + "SQL Server. Implement IEventStore and IKpiStore against it before switching the provider.");
        }

        if (!string.Equals(provider, Sqlite, StringComparison.OrdinalIgnoreCase))
        {
            throw new NotSupportedException($"Unknown Storage:Provider '{provider}'. Expected '{Sqlite}' or '{SqlServer}'.");
        }

        services.AddSingleton(sp =>
            new SqliteDb(sp.GetRequiredService<IOptions<StorageOptions>>().Value.SqlitePath));
        services.AddSingleton<IEventStore, SqliteEventStore>();
        services.AddSingleton<IKpiStore, SqliteKpiStore>();
    }
}
