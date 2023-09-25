using Microsoft.EntityFrameworkCore;

namespace TestHost;

public class TestHostContext : DbContext
{
    public TestHostContext(DbContextOptions options) : base(options) { }

    public DbSet<Log> Logs { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Log>();

        base.OnModelCreating(modelBuilder);
    }

}

public class Log
{
    public int Id { get; set; }
    public string Host { get; set; } = null!;
    public string Message { get; set; } = null!;
}
