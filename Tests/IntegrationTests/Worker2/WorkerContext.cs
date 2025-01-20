using Microsoft.EntityFrameworkCore;

namespace Worker2;

public class WorkerContext : DbContext
{
    public WorkerContext(DbContextOptions options) : base(options) { }

    public DbSet<Result> Results { get; set; }
    public DbSet<Stats> Stats { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Result>(o =>
        {
            o.ToTable("results");
            o.HasKey(x => x.Id);

            o.Property(o => o.Id).HasColumnName("id");
            o.Property(o => o.MessageId).HasColumnName("message_id");
            o.Property(o => o.Partition).HasColumnName("t_partition");
            o.Property(o => o.Offset).HasColumnName("t_offset");
            o.Property(o => o.Topic).HasColumnName("topic");
        });

        modelBuilder.Entity<Log>(o =>
        {
            o.ToTable("logs");
            o.HasKey(x => x.Id);
        });

        modelBuilder.Entity<Stats>(o =>
        {
            o.ToTable("stats");
        });

        base.OnModelCreating(modelBuilder);
    }

}

public class Result
{
    public int Id { get; set; }
    public string MessageId { get; set; } = null!;
    public int Partition { get; set; }
    public int Offset { get; set; }
    public string Host { get; set; } = null!;
    public string Topic { get; set; } = null!;
}

public class Log
{
    public int Id { get; set; }
    public string Host { get; set; } = null!;
    public string Message { get; set; } = null!;
}

public class Stats
{
    public int Id { get; set; }
    public string Host { get; set; } = null!;
    public string Value { get; set; } = null!;
    public DateTime CreatedAt { get; set; }
}
