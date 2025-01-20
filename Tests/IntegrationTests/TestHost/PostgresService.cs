using Testcontainers.PostgreSql;

sealed class PostgresService : IContainerInit
{
    private readonly PostgreSqlContainer _postgres;

    public PostgresService()
    {
        _postgres = new PostgreSqlBuilder()
            .WithDatabase("postgres")
            .WithUsername("user")
            .WithPassword("password")
            .WithPortBinding(5432, 5432)
            .Build();
    }

    public async Task InitAsync(CancellationToken token)
    {
        const string sql = """
            create table results(
                id bigint primary key generated always as identity,
                host text not null,
                message_id text not null,
                topic text not null,
                t_partition int not null,
                t_offset int not null);

            create table logs(
                id bigint primary key generated always as identity,
                host text not null,
                message text not null,
                created_at timestamptz default now());

            create table stats(
                id bigint primary key generated always as identity,
                host text not null,
                value text not null,
                created_at timestamptz default now());
            """;

        await _postgres.StartAsync(token);
        await _postgres.ExecScriptAsync(sql, token);
    }

    public async ValueTask DisposeAsync()
    {
        await _postgres.DisposeAsync();
    }
}
