using System.Net;
using LinePulse.Core.Api;
using Microsoft.AspNetCore.Http;

namespace LinePulse.Tests;

/// <summary>
/// Which requests count as a visit, and what the line says about one.
/// </summary>
/// <remarks>
/// The dashboard polls the API every few seconds and the platform checks health every
/// thirty; if either were logged, the few page loads the log exists for would be lost in
/// the noise.
/// </remarks>
public class VisitLogTests
{
    private static HttpContext Request(string method, string path, params (string Name, string Value)[] headers)
    {
        var context = new DefaultHttpContext();
        context.Request.Method = method;
        context.Request.Path = path;
        foreach (var (name, value) in headers)
        {
            context.Request.Headers[name] = value;
        }
        return context;
    }

    [Theory]
    [InlineData("GET", "/", true)]
    [InlineData("GET", "/index.html", true)]
    [InlineData("GET", "/lines/IZM-L01", true)]
    [InlineData("GET", "/api/health", false)]
    [InlineData("GET", "/api/lines", false)]
    [InlineData("GET", "/app.js", false)]
    [InlineData("GET", "/favicon.ico", false)]
    [InlineData("HEAD", "/", false)]
    [InlineData("POST", "/", false)]
    public void Only_page_loads_count(string method, string path, bool expected)
    {
        Assert.Equal(expected, VisitLog.IsPageLoad(Request(method, path).Request));
    }

    [Fact]
    public void Line_carries_the_proxy_headers_and_only_the_referrer_host()
    {
        var context = Request("GET", "/",
            ("Fly-Region", "otp"),
            ("Fly-Client-IP", "203.0.113.7"),
            ("Referer", "https://outlook.office.com/mail/inbox?token=secret"),
            ("User-Agent", "Mozilla/5.0 \"Test\""));

        Assert.Equal(
            "visit region=otp ip=203.0.113.7 path=/ ref=outlook.office.com ua=\"Mozilla/5.0 'Test'\"",
            VisitLog.Describe(context));
    }

    [Fact]
    public void Without_the_proxy_the_socket_address_stands_in()
    {
        var context = Request("GET", "/");
        context.Connection.RemoteIpAddress = IPAddress.Loopback;

        Assert.Equal("visit region=- ip=127.0.0.1 path=/ ref=- ua=\"-\"", VisitLog.Describe(context));
    }

    [Fact]
    public void Long_user_agent_is_cut()
    {
        var context = Request("GET", "/", ("User-Agent", new string('x', 500)));

        Assert.Contains($"ua=\"{new string('x', 200)}\"", VisitLog.Describe(context));
    }
}
