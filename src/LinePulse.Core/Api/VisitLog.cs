namespace LinePulse.Core.Api;

/// <summary>
/// One log line per page load of the dashboard, so the owner of the live demo can tell
/// whether, when and from roughly where it was opened.
/// </summary>
/// <remarks>
/// Only page loads are logged: API polling, static assets and the platform's health
/// checks would bury the handful of lines that matter. The line goes to stdout, where
/// the hosting platform keeps it searchable for about a week; nothing is written to the
/// database, which is wiped on every start anyway. <c>Fly-Client-IP</c> and
/// <c>Fly-Region</c> are set by the Fly proxy (the region is the edge that accepted the
/// connection, not where the app runs). Run locally, the socket address stands in and the
/// region is "-". The referrer is reduced to its host so a query string carrying a token
/// never reaches the log.
/// </remarks>
public static class VisitLog
{
    private const int MaxUserAgentLength = 200;

    /// <summary>True when the request is a browser opening a page, not the page fetching data.</summary>
    public static bool IsPageLoad(HttpRequest request)
    {
        if (!HttpMethods.IsGet(request.Method))
        {
            return false;
        }

        var path = request.Path.Value ?? "/";
        if (path.StartsWith("/api", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        // "/" and deep links have no extension; index.html is the page itself.
        var extension = Path.GetExtension(path);
        return extension.Length == 0 || extension.Equals(".html", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>The log line for a page load.</summary>
    public static string Describe(HttpContext context)
    {
        var request = context.Request;
        var region = Header(request, "Fly-Region") ?? "-";
        var ip = Header(request, "Fly-Client-IP") ?? context.Connection.RemoteIpAddress?.ToString() ?? "-";
        var referrer = ReferrerHost(Header(request, "Referer"));
        var userAgent = Header(request, "User-Agent") ?? "-";
        if (userAgent.Length > MaxUserAgentLength)
        {
            userAgent = userAgent[..MaxUserAgentLength];
        }

        return $"visit region={region} ip={ip} path={request.Path.Value ?? "/"} " +
               $"ref={referrer} ua=\"{userAgent.Replace("\"", "'")}\"";
    }

    public static IApplicationBuilder UseVisitLog(this IApplicationBuilder app)
    {
        var logger = app.ApplicationServices.GetRequiredService<ILoggerFactory>().CreateLogger("LinePulse.Visits");
        return app.Use(async (context, next) =>
        {
            if (IsPageLoad(context.Request))
            {
                logger.LogInformation("{Visit}", Describe(context));
            }
            await next(context);
        });
    }

    private static string? Header(HttpRequest request, string name)
    {
        var value = request.Headers[name].ToString();
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }

    private static string ReferrerHost(string? referrer) =>
        referrer is not null && Uri.TryCreate(referrer, UriKind.Absolute, out var uri) ? uri.Host : "-";
}
