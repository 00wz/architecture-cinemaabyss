var monolithUrl      = Environment.GetEnvironmentVariable("MONOLITH_URL")      ?? "http://localhost:8080";
var moviesServiceUrl = Environment.GetEnvironmentVariable("MOVIES_SERVICE_URL") ?? "http://localhost:8081";
var eventsServiceUrl = Environment.GetEnvironmentVariable("EVENTS_SERVICE_URL") ?? "http://localhost:8082";

var gradualMigration = string.Equals(
    Environment.GetEnvironmentVariable("GRADUAL_MIGRATION"), "true",
    StringComparison.OrdinalIgnoreCase);

var moviesMigrationPercent = int.TryParse(
    Environment.GetEnvironmentVariable("MOVIES_MIGRATION_PERCENT"), out var p) ? p : 0;

// ── hop-by-hop заголовки не пробрасываем ───────────────────────────────────
var hopByHop = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    { "Connection", "Keep-Alive", "Transfer-Encoding", "TE", "Trailer",
      "Upgrade", "Proxy-Authorization", "Proxy-Authenticate" };

// ── настройка приложения ───────────────────────────────────────────────────
var builder = WebApplication.CreateBuilder(args);

var port = Environment.GetEnvironmentVariable("PORT") ?? "8000";
builder.WebHost.UseUrls($"http://+:{port}");
builder.Services.AddHttpClient("proxy", c => c.Timeout = TimeSpan.FromSeconds(30));

var app = builder.Build();

// ── health-check ───────────────────────────────────────────────────────────
app.MapGet("/health", () => Results.Ok(new { status = true }));

// ── главный catch-all: перехватывает всё кроме зарегистрированных эндпоинтов
app.MapFallback(async (HttpContext ctx) =>
{
    var path  = ctx.Request.Path.Value  ?? "/";
    var query = ctx.Request.QueryString.Value ?? "";

    // определяем целевой бэкенд
    string targetBase;
    if (path.StartsWith("/api/movies"))
    {
        var toMoviesService = gradualMigration && Random.Shared.Next(100) < moviesMigrationPercent;
        targetBase = toMoviesService ? moviesServiceUrl : monolithUrl;
        app.Logger.LogInformation("[proxy] {Path} → {Target} (migration {Pct}%, gradual={Flag})",
            path, toMoviesService ? "movies-service" : "monolith",
            moviesMigrationPercent, gradualMigration);
    }
    else if (path.StartsWith("/api/events"))
    {
        targetBase = eventsServiceUrl;
        app.Logger.LogInformation("[proxy] {Path} → events-service", path);
    }
    else
    {
        targetBase = monolithUrl;
        app.Logger.LogInformation("[proxy] {Path} → monolith", path);
    }

    var targetUri = new Uri(targetBase.TrimEnd('/') + path + query);

    // собираем исходящий запрос
    using var req = new HttpRequestMessage(new HttpMethod(ctx.Request.Method), targetUri);

    foreach (var (key, value) in ctx.Request.Headers)
        if (!hopByHop.Contains(key) && !key.Equals("Host", StringComparison.OrdinalIgnoreCase))
            req.Headers.TryAddWithoutValidation(key, (IEnumerable<string>)value);

    if (HttpMethods.IsPost(ctx.Request.Method)  ||
        HttpMethods.IsPut(ctx.Request.Method)   ||
        HttpMethods.IsPatch(ctx.Request.Method) ||
        ctx.Request.ContentLength > 0)
    {
        var content = new StreamContent(ctx.Request.Body);
        if (!string.IsNullOrEmpty(ctx.Request.ContentType))
            content.Headers.TryAddWithoutValidation("Content-Type", ctx.Request.ContentType);
        req.Content = content;
    }

    // отправляем и пробрасываем ответ
    var factory = ctx.RequestServices.GetRequiredService<IHttpClientFactory>();
    using var resp = await factory.CreateClient("proxy")
        .SendAsync(req, HttpCompletionOption.ResponseHeadersRead);

    ctx.Response.StatusCode = (int)resp.StatusCode;

    foreach (var (key, value) in resp.Headers)
        if (!hopByHop.Contains(key))
            ctx.Response.Headers[key] = value.ToArray();

    foreach (var (key, value) in resp.Content.Headers)
        if (!hopByHop.Contains(key))
            ctx.Response.Headers[key] = value.ToArray();

    await resp.Content.CopyToAsync(ctx.Response.Body);
});

app.Run();

