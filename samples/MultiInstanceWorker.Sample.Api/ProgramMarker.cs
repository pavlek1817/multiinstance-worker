// Exposed so MultiInstanceWorker.FunctionalTests can host this app via WebApplicationFactory<Program>.
// Kept in its own file (rather than appended to Program.cs) because StyleCop mis-locates
// SA1516 when a type declaration follows top-level statements in the same file.
public partial class Program
{
}
